package ProgQueries

import ProgQueries.entryPoint.{excludedFiles, getListOfFiles2, sample}
import Query.QueryParser
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{array, lit, struct, udf}

import java.io.File
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}


object MultiTableQueryExecutor {

  /*
  Method to execute a query on multiple tables progressively.
  @param spark, the current spark Session
  @param query, String of sql query
  @param joinInputs, array of triples where each triple is the form (table1, table2, joinAttribute)
  @param params: query and evaluation parameters
  @param agg: type of aggregate function and column its applied on
  @return nothing
   */

  def runMultiTableQuery(spark: SparkSession, query: String, params: Map[String, String],
                         queryStorage: Map[String,Any], qID: String): Unit = {

    val b = params("b").toInt
    // Error tolerance, i.e. when to stop
    val errorTol = params("errorTol").toDouble
    // Keep track of error at each iteration
    val currentErrors = ArrayBuffer[Double]()
    val Eval = new Evaluator()
    val qp = new QueryParser()

    // All results computed so far
    var runningResult = spark.emptyDataFrame
    val tableNames = qp.getTables(spark, query)

    // Query rewrite depends if the query has a group by clause
    val logicalPlan = spark.sessionState.sqlParser.parsePlan(query)

    val joinInputs = qp.parseQueryJoin(spark, query)
    val groupBys = qp.parseQueryGrouping(logicalPlan)
    val hasGroupBy:Boolean = query.contains("group by")
    val aggregates: ArrayBuffer[(String, String, String)] = qp.parseQueryAggregate(logicalPlan, hasGroupBy)

    // TODO: Turn this into an sql function
    val sidUDF = udf(h _)

    val errors = ListBuffer.empty[ListBuffer[(String, String, String, Double)]]
    val estimates = ListBuffer.empty[ListBuffer[(String, Double)]]

    // remove aggregates and group by
    var queryWithoutAgg = query
    for (agg <- aggregates) {
      queryWithoutAgg = queryWithoutAgg.replace(agg._1 + "(" + agg._2 + ")" + " as " + agg._3, agg._2 + " as " + agg._3)
    }

    // Remove group by, since we removed aggregates
    if (hasGroupBy)
      queryWithoutAgg = queryWithoutAgg.substring(0, queryWithoutAgg.indexOf("group"))


    val joinGraph = new JoinGraphV2()
    val Stats = new StatisticsBuilder(spark)
    Stats.loadCMS()
    val partitionPathsMap: mutable.Map[String, List[File]] = mutable.Map()

    for ((table, index) <- tableNames.zipWithIndex){
      val partitionPaths = getListOfFiles2("data_parquet/" + table, excludedFiles)
      partitionPathsMap += (table -> partitionPaths)

      for (path <- partitionPaths){
        val nType = if (index == 0) "start" else if (index == tableNames.length - 1) "end" else "interm"
        val vertex = new joinGraph.Node(path.getName, nType, table)
        joinGraph.addVertex(vertex)
      }
    }

    for (ji <- joinInputs)
      joinGraph.addEdges(ji._1, ji._2)


    val paths: ListBuffer[ListBuffer[Set[joinGraph.Node]]] = joinGraph.calculatePaths()
    val weightedPaths: Map[ListBuffer[Set[joinGraph.Node]], Double] = Map()
    // TODO: change to calculate cms inner product only once for each unique combination
    // IDEA: maybe use some decision tree?

    for (path <- paths){
      var size = 1

      for (i <- 0 to path.length - 2){
        val leftNodeName = path(i).head.getName()
        val leftNodeKey = joinInputs(i)._3
        val leftNodeTable = path(i).head.getTableName()
        val rightNodeName = path(i+1).head.getName()
        val rightNodeKey = joinInputs(i)._4
        val rightNodeTable = path(i+1).head.getTableName()

        val cmsLeft = Stats.getCMS(leftNodeTable, leftNodeName, leftNodeKey)
        val cmsRight = Stats.getCMS(rightNodeTable, rightNodeName, rightNodeKey)
        val joinSize = cmsLeft.innerProduct(cmsRight).estimate.toDouble

        if (joinSize == 0)
          size = 0
      }

      // assign weights better
      if (size == 1){
        weightedPaths += (path -> 1.0)
      }

    }

    // Cache loaded partitions, as well as which partitions have been loaded
    val dfCache: Map[String, DataFrame] = Map()
    val partitionNameCache: Map[String,ListBuffer[String]] = Map()
    for (table <- tableNames) {
      dfCache += (table -> spark.emptyDataFrame)
      partitionNameCache += (table -> ListBuffer())
    }


    var i = 0
    val maxIterations: Integer = 5
    while (i < maxIterations) {

      val path = sample(weightedPaths)
      joinGraph.printPath(path)
      weightedPaths -= path

      for ((table, index) <- tableNames.zipWithIndex){
        val partitionDF = spark.read.parquet("data_parquet/" + table + "/" +
          path(index).head.getName())

        /*
        if (dfCache(table).isEmpty)
          dfCache(table) = partitionDF
        else if (!partitionNameCache(table).contains(path(index).head.getName()))
          dfCache(table) = dfCache(table).union(partitionDF)

        partitionNameCache(table) += path(index).head.getName()
        dfCache(table).createOrReplaceTempView(table)
         */
        partitionDF.createOrReplaceTempView(table)

      }

      val partial = spark.sql(queryWithoutAgg)

      /*
      Step 1: Join all tuples, without computing aggregate, so that we can compute their combined sid
       */
      // Assign sid's to result tuples
      val sidColumns: Array[String] = partial.schema.fieldNames.filter( col => col.contains("_sid"))
      var resultWithSid = partial.withColumn("sid", sidUDF(lit(b), struct(partial.columns map col: _*),
        array(sidColumns.map(lit(_)): _*)))

      // Result with newly assigned SIDs
      for (sid <- sidColumns) {
        resultWithSid = resultWithSid.drop(sid)
      }


      // Add new samples to old ones
      if ( i == 0 )
        runningResult = resultWithSid
      else
        runningResult = runningResult.union(resultWithSid)

      runningResult.createOrReplaceTempView("result")
      println(resultWithSid.count())
      println(runningResult.count())

      /*
      Step 2: Compute the aggregates on the joined tuples, group by sid
       */
      val groupings: Seq[String] = qp.parseQueryGrouping(logicalPlan)
      val cols: Seq[String] = resultWithSid.columns.toSeq
      var aggQueryWithSid = "select "


      // Hack to check if a column is an aggregate : check if it contains a bracket (
      for (col <- cols){
        aggQueryWithSid = aggQueryWithSid + (if(aggregates.map(_._3).contains(col)) aggregates.find(_._3 == col).get._1 + "(" +
          col + ")" + " as " + col else col) + ","
      }
      // Delete the last comma
      aggQueryWithSid = aggQueryWithSid.dropRight(1)


      if (hasGroupBy)
        aggQueryWithSid = aggQueryWithSid + " from result group by " + groupings.mkString(",") + ", sid"

      else
        aggQueryWithSid = aggQueryWithSid + " from result group by sid"


      val resultAggregatedWithSid = spark.sql(aggQueryWithSid)
      resultAggregatedWithSid.createOrReplaceTempView("result")
      val aggregatesNoSid = aggregates.filterNot(_._3.contains("sid"))

      // Evaluate new result
      // scale factor
      // val sf = calculateSF(sampleChoices, i)
      val sf = 1.0

      val res = Eval.evaluatePartialResult(resultAggregatedWithSid, params, aggregatesNoSid, sf)

      val errorsForIter = ListBuffer.empty[(String, String, String, Double)]

      //  var resultMap = scala.collection.mutable.Map[String,Any]()

      val estForIter = ListBuffer.empty[(String, Double)]

      for (evalResult <- res) {
        val agg = evalResult("agg")
        val est = evalResult("est")
        val groupError = evalResult("error").toDouble
        val group = evalResult("group")
        errorsForIter.append((agg, group, est, groupError))
        currentErrors.append(groupError)

        /*
        val resultPerGroup = scala.collection.mutable.Map[String,String]()
        resultPerGroup += ("Estimate" -> evalResult("est"))
        resultPerGroup += ("CI_low" -> evalResult("ci_low"))
        resultPerGroup += ("CI_high" -> evalResult("ci_high"))
        resultPerGroup += ("Error" -> groupError.toString)

        resultMap += ((evalResult("agg"), evalResult("group")).toString() ->
          scala.util.parsing.json.JSONObject(resultPerGroup.toMap))

         */
        estForIter.append((group, est.toDouble))

      }

      //queryStorage += (qID -> scala.util.parsing.json.JSONObject(resultMap.toMap))

      // if (currentErrors.count(_ < errorTol) == currentErrors.length)
      //  break

      println(errorsForIter.mkString("\n"))

      currentErrors.clear()
      estForIter.clear()
      errors.append(errorsForIter)
      estimates.append(estForIter)

      i += 1
    }

  }


  /*
  h(i,j,..) function to calculated the new sid from VerdictDB
  */
  def h( b: Integer, row: Row, sidFields: Seq[String]): String = {
    val joinSize = sidFields.size
    var h = 1.0

    for (i <- 0 until joinSize){
      val sid = row.getAs(sidFields(i)).toString.toDouble
      if (sid == 0)
        return "0"
      else
        h += Math.floor((sid-1) / math.sqrt(b.doubleValue())) * math.pow(math.sqrt(b.doubleValue()), joinSize - 1 - i)
    }
    h = Math.floor(h / math.pow(math.sqrt(b.doubleValue()), joinSize - 2))

    h.toString
  }

}
