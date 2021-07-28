package Bench

import FileHandlers.StatisticsBuilder
import PartitionLogic.PartitionPickerSingleTable
import ProgQueries.ResultCombiner
import ProgQueries.entryPoint.{excludedFiles, getListOfFiles2, sample}
import Query.QueryParser
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}


object SingleTableQueryExecutorRl {

  /*
  Method to execute a query on a single table progressively. Single aggregate functions only
  @param spark, the current spark Session
  @param query, String of sql query
  @param params: query and evaluation parameters
  @param agg: type of aggregate function and column its applied on
  @return nothing
   */
  def runSingleTableQuery(spark: SparkSession, query: String, dataDir: String,
                          exactResult: Map[List[String], List[(String, String)]]): ListBuffer[(Double, Double)] ={


    // Keep track of error at each iteration
    val currentErrors = ArrayBuffer[Double]()

    val qp = new QueryParser()

    // Get names of all subdirectories in tableName.parquet dir
    val tableName = qp.getTables(spark, query).head

    val p = 0.01

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
    val groupingCols = qp.parseQueryGrouping(logicalPlan).toList
    val resultCombiner = new ResultCombiner()

    val maxIterations = 10
    var i = 0

    // errors per iteration
    val errorsPerIter: ListBuffer[(Double, Double)] = ListBuffer()
    val foundGroups: ListBuffer[String] = ListBuffer()

    // TODO: have to run 2 queries - one for real result, other for subsamples
    while (i < maxIterations) {

      val partitions = spark.read.parquet(dataDir + tableName).sample(p)

      partitions.createOrReplaceTempView(tableName)
      val partial = spark.sql(newQuery)

      // Combine and evaluate new result
      val sf = 1.0 / ((i.toDouble + 1.0) / 100.0)
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
      currentErrors.clear()

      i += 1

      println("*****Iteration " + i + " complete*****")
    }

    errorsPerIter

  }

}
