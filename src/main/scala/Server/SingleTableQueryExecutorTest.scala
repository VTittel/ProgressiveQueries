package Server

import FileHandlers.StatisticsBuilder
import PartitionLogic.PartitionPickerSingleTable
import ProgQueries.ResultCombiner
import ProgQueries.entryPoint.{excludedFiles, getListOfFiles2, sample}
import Query.QueryParser
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}


object SingleTableQueryExecutorTest {

  /*
  Method to execute a query on a single table progressively. Single aggregate functions only
  @param spark, the current spark Session
  @param query, String of sql query
  @param params: query and evaluation parameters
  @param agg: type of aggregate function and column its applied on
  @return nothing
   */
  def runSingleTableQuery(spark: SparkSession, query: String, dataDir: String, errorTol: Double,
                          queryStorage: Map[String,Any], qID: String): ListBuffer[(Double, Double)] ={


    // Keep track of error at each iteration
    val currentErrors = ArrayBuffer[Double]()

    val qp = new QueryParser()
    val queryWithSid = addSid(query)

    // Get names of all subdirectories in tableName.parquet dir
    val tableName = qp.getTables(spark, query).head

    val p = 0.01

    // Query rewrite depends if the query has a group by clause
    // TODO: add sid calculation to query automatically
    var newQuery = ""
    val hasGroupBy:Boolean = query.contains("group by")

    if (hasGroupBy)
      newQuery = addSid(query) + ", sid"
    else
      newQuery = addSid(query) + " group by " + " sid"

    val logicalPlan = spark.sessionState.sqlParser.parsePlan(newQuery)
    val aggregates: ArrayBuffer[(String, String, String)] = qp.parseQueryAggregate(logicalPlan, hasGroupBy)
    val groupingCols = qp.parseQueryGrouping(logicalPlan).toList
    val resultCombiner = new ResultCombiner()

    val maxIterations = 15
    var i = 0

    // errors per iteration
    val errorsPerIter: ListBuffer[(Double, Double)] = ListBuffer()

    // TODO: have to run 2 queries - one for real result, other for subsamples
    while (i < maxIterations) {

      val partitions = spark.read.parquet(dataDir + tableName).sample(p)
      partitions.createOrReplaceTempView(tableName)
      val partial = spark.sql(newQuery)

      // Combine and evaluate new result

      val sf = 1.0 / ((i.toDouble + 1.0) / 100.0)
      val partialResult = resultCombiner.combine(partial, aggregates, sf)

      val resultMap = scala.collection.mutable.Map[String,Any]()
      for (res <- partialResult){
        if (res._1._1 != "count_tuples"){
          val agg = res._1._1
          val group = res._1._2
          val err = res._2("error")
          val est = res._2("est")
          val ciLow = res._2("ci_low")
          val ciHigh = res._2("ci_high")
          val count = res._2("count")
          val resultPerGroup = scala.collection.mutable.Map[String,String]()

          resultPerGroup += ("Estimate" -> est)
          resultPerGroup += ("CI_low" -> ciLow)
          resultPerGroup += ("CI_high" -> ciHigh)
          resultPerGroup += ("Error" -> err)
          resultPerGroup += ("Count" -> count)

          resultMap += ((agg, group).toString() ->
            scala.util.parsing.json.JSONObject(resultPerGroup.toMap))
        }
      }

      currentErrors.clear()

      queryStorage += (qID -> scala.util.parsing.json.JSONObject(resultMap.toMap))
      i += 1
      println("*****Iteration " + i + " complete*****")
    }

    errorsPerIter

  }


  def addSid(query: String): String = {
    val (fst, snd) = query.splitAt(query.indexOf("from"))
    val toAdd = ",count(*) as count_tuples, if (1+ floor ( rand () * 1000) <= 100, 1 + floor(rand()*100), 0) as sid "
    val rewritten = fst + toAdd + snd
    rewritten
  }

}
