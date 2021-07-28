package ProgQueries

import FileHandlers.StatisticsBuilder
import PartitionLogic.PartitionPickerSingleTable
import ProgQueries.entryPoint.{excludedFiles, getListOfFiles2, sample}
import Query.QueryParser
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}


object SingleTableQueryExecutor {

  /*
  Method to execute a query on a single table progressively. Single aggregate functions only
  @param spark, the current spark Session
  @param query, String of sql query
  @param params: query and evaluation parameters
  @param agg: type of aggregate function and column its applied on
  @return nothing
   */
  def runSingleTableQuery(spark: SparkSession, query: String, dataDir: String, errorTol: Double,
                          exactResult: Map[List[String], List[(String, String)]],
                          statBuilder : StatisticsBuilder,
                          queryStorage: Map[String,Any], qID: String,
                          exprs: List[List[(String, String, String, String)]],
                          innerConnector: String, outerConnector: String,
                          predCols: List[String], isRandom: Boolean): ListBuffer[(Double, Double)] ={


    // Keep track of error at each iteration
    val currentErrors = ArrayBuffer[Double]()

    val qp = new QueryParser()

    // Get names of all subdirectories in tableName.parquet dir
    val tableName = qp.getTables(spark, query).head
    val numPartitions = getListOfFiles2(dataDir + tableName, excludedFiles).size
    val p = math.ceil(numPartitions * 0.01)

    // Query rewrite depends if the query has a group by clause
    // TODO: add sid calculation to query automatically
    var newQuery = ""
    val hasGroupBy:Boolean = query.contains("group by")

    if (hasGroupBy)
      newQuery = query + ", sid"
    else
      newQuery = query + " group by " + " sid"

    val logicalPlan = spark.sessionState.sqlParser.parsePlan(query)
    val aggregates: ArrayBuffer[(String, String, String)] = qp.parseQueryAggregate(logicalPlan, hasGroupBy)
    val pp = new PartitionPickerSingleTable(spark, dataDir, statBuilder)

    val groupingCols = qp.parseQueryGrouping(logicalPlan).toList
    val partitionObjects = pp.createPartitions(tableName, hasPred = false, exprs, innerConnector, outerConnector,
      predCols, groupingCols, isRandom)
    val resultCombiner = new ResultCombiner()
    //   val execTimes = scala.collection.mutable.ListBuffer.empty[Double]

    val maxIterations = 10
    var i = 0
    // Total table size
    val statDir = "Statistics_" + dataDir.slice(dataDir.indexOf("sf"), dataDir.length)
    val totalSize = statBuilder.getStatAsString(statDir + "/" + tableName + "/totalnumtuples.txt").toLong
    // sample size so far
    var sampleSizeSoFar = 0L
    // errors per iteration
    val errorsPerIter: ListBuffer[(Double, Double)] = ListBuffer()
    val foundGroups: ListBuffer[String] = ListBuffer()

    // TODO: have to run 2 queries - one for real result, other for subsamples
    while (i < maxIterations) {
      // val startTime = System.nanoTime
      // sample partitions with budget
      val partitionNames: ListBuffer[String] = ListBuffer()
      val partObjectSamples = pp.samplePartitions(partitionObjects, p.toInt)
      // calculate number of tuples in all partitions
      for (elem <- partObjectSamples) {
        partitionNames.append(dataDir + elem.pTable + "/" + elem.pName)
       // sampleSizeSoFar += elem.getPartSize()
      }
      sampleSizeSoFar += partObjectSamples.size

      val partitions = spark.read.parquet(partitionNames: _*)

      partitions.createOrReplaceTempView(tableName)
      val partial = spark.sql(newQuery)

      // Combine and evaluate new result
      // scale factor
   //   val sf = 1.0 / (sampleSizeSoFar.toDouble / totalSize.toDouble)
      val sf = 1.0 / (sampleSizeSoFar.toDouble / numPartitions.toDouble)
      val partialResult = resultCombiner.combine(partial, aggregates, sf)

      var avgError = 0.0
      val trueNumGroups = exactResult.size.toDouble
      val numAggs = aggregates.size.toDouble

      // for each group
      for ((group, aggs) <- exactResult) {
        // for each aggregate in the group
        val groupString = group.mkString(",")

        // check if group is missing
        if (partialResult.contains((aggs.head._1, groupString))){
          for (aggval <- aggs){
            val aggName = aggval._1
            val aggTrueValue = aggval._2.toDouble

            val aggEstValue = partialResult((aggName, groupString))("est").toDouble
            val error = math.abs(aggEstValue - aggTrueValue) / aggTrueValue
            avgError += error
          }

          foundGroups.append(groupString)

        } else {
          // if group is missing, error is 1
          avgError += 1.0
        }
      }

      avgError = avgError / (trueNumGroups * numAggs)
      val missingGroupPct = 1.0 - foundGroups.toSet.size.toDouble / trueNumGroups

      errorsPerIter.append((avgError, missingGroupPct))

      /*
      val resultMap = scala.collection.mutable.Map[String,Any]()
      val resultPerGroup = scala.collection.mutable.Map[String,String]()
      resultPerGroup += ("Estimate" -> evalResult("est"))
      resultPerGroup += ("CI_low" -> evalResult("ci_low"))
      resultPerGroup += ("CI_high" -> evalResult("ci_high"))
      resultPerGroup += ("Error" -> groupError.toString)

      resultMap += ((evalResult("agg"), evalResult("group")).toString() ->
        scala.util.parsing.json.JSONObject(resultPerGroup.toMap))

       */

      //queryStorage += (qID -> scala.util.parsing.json.JSONObject(resultMap.toMap))

      // Break if accuracy is achieved
      // TODO: Fix for groups that have too little tuples, and thus 0 error
      // if (currentErrors.count(_ < errorTol) == currentErrors.length)
      //   break

      currentErrors.clear()

      i += 1

     // println("*****Iteration " + i + " complete*****")
    }

    errorsPerIter
    //println(execTimes.mkString(","))

  }

}