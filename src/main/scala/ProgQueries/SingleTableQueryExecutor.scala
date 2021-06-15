package ProgQueries

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
  def runSingleTableQuery(spark: SparkSession, query: String, params: Map[String, String],
                          queryStorage: Map[String,Any], qID: String): Unit ={

    val Stats = new StatisticsBuilder(spark)
    Stats.loadCMS()

    // Error tolerance, i.e. when to stop
    val errorTol = params("errorTol").toDouble

    // Keep track of error at each iteration
    val currentErrors = ArrayBuffer[Double]()

    var partitionCache = spark.emptyDataFrame
    val evaluator = new Evaluator()
    val qp = new QueryParser()


    // Get names of all subdirectories in tableName.parquet dir
    val tableName = qp.getTables(spark, query).head

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
    val partitions = getListOfFiles2("data_parquet/" + tableName, excludedFiles)

    //   val execTimes = scala.collection.mutable.ListBuffer.empty[Double]
    val errors = ListBuffer.empty[ListBuffer[(String, String, String, Double)]]

    // Eliminate all partitions that dont pass selection predicate
    // map all to 1 for now
    val weightedPartitions: Map[String, Double] = Map()

    for (part <- partitions){
      weightedPartitions += (part.toString -> 1.0)
    }

    val numPartitions = partitions.length.toDouble
    val maxIterations = 3
    var i = 0

    // TODO: have to run 2 queries - one for real result, other for subsamples
    while (i < maxIterations) {
      // val startTime = System.nanoTime
      // sample partition
      val partitionName = sample(weightedPartitions)
      weightedPartitions -= partitionName

      val partition = spark.read.parquet(partitionName)
      // val partition = spark.read.parquet("data_parquet/lineitem").sample(0.02)

      if (partitionCache.isEmpty)
        partitionCache = partition
      else
        partitionCache = partitionCache.union(partition)

      partitionCache.createOrReplaceTempView(tableName)
      val partial = spark.sql(newQuery)

      // Evaluate new result
      // scale factor
      val sf = 1.0 / ((i+1) / numPartitions)
      val res = evaluator.evaluatePartialResult(partial, params, aggregates, sf)
      val errorsForIter = ListBuffer.empty[(String, String, String, Double)]

      val resultMap = scala.collection.mutable.Map[String,Any]()

      for (evalResult <- res) {
        val agg = evalResult("agg")
        val est = evalResult("est")
        val groupError = evalResult("error").toDouble
        val group = evalResult("group")

        errorsForIter.append((agg, group, est, groupError))
        currentErrors.append(groupError)

        val resultPerGroup = scala.collection.mutable.Map[String,String]()

        resultPerGroup += ("Estimate" -> evalResult("est"))
        resultPerGroup += ("CI_low" -> evalResult("ci_low"))
        resultPerGroup += ("CI_high" -> evalResult("ci_high"))
        resultPerGroup += ("Error" -> groupError.toString)

        resultMap += ((evalResult("agg"), evalResult("group")).toString() ->
          scala.util.parsing.json.JSONObject(resultPerGroup.toMap))
      }

      queryStorage += (qID -> scala.util.parsing.json.JSONObject(resultMap.toMap))

      // Break if accuracy is achieved
      // TODO: Fix for groups that have too little tuples, and thus 0 error
      // if (currentErrors.count(_ < errorTol) == currentErrors.length)
      //   break

      currentErrors.clear()
      errors.append(errorsForIter)
      println(errorsForIter.mkString("\n"))

      i += 1

      println("*****Iteration " + i + " complete*****")

    }

    //println(execTimes.mkString(","))


  }

}
