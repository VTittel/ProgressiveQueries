package ProgQueries

import FileHandlers.StatisticsBuilder
import PartitionLogic.{JoinGraphV2, PartitionPickerMultiTable}
import ProgQueries.entryPoint.{excludedFiles, getListOfFiles2, sample}
import Query.{BenchQueries, QueryParser}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{array, col, lit, struct, udf}

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

  // total number of partitions sampled so far
  val partitionCounts: Map[String, Int] = Map()

  def runMultiTableQuery(spark: SparkSession, query: String, dataDir: String, errorTol: Double,
                         exactResult: Map[List[String], List[(String, String)]],
                         statBuilder : StatisticsBuilder,
                         queryStorage: Map[String,Any], qID: String,
                         exprs: Map[String, List[List[(String, String, String, String)]]],
                         innerConnector: String, outerConnector: String,
                         predCols: Map[String, List[String]],
                         groupingCols: Map[String, List[String]],
                         isRandom: Boolean):  ListBuffer[(Double, Double)] = {


    // Keep track of error at each iteration
    val b = 100
    val qp = new QueryParser()
    val resultCombiner = new ResultCombiner()


    val tableNames = qp.getTables(spark, query)
    var p = 0.0
    var totalNumPartitions = 0.0
    for (table <- tableNames) {
      val nump = getListOfFiles2(dataDir + table, excludedFiles).size
      p += math.ceil(nump * 0.01)
      totalNumPartitions += nump.toDouble
    }

    // Query rewrite depends if the query has a group by clause
    val logicalPlan = spark.sessionState.sqlParser.parsePlan(query)

    val joinInputs = qp.parseQueryJoin(spark, query)
    val hasGroupBy:Boolean = query.contains("group by")
    val aggregates: ArrayBuffer[(String, String, String)] = qp.parseQueryAggregate(logicalPlan, hasGroupBy)

    // TODO: Turn this into an sql function
    val sidUDF = udf(h _)

    // remove aggregates and group by
    var queryWithoutAgg = query

    for (agg <- aggregates) {
      if (agg._2.head.toString == "(" && agg._2.last.toString == ")")
        queryWithoutAgg = queryWithoutAgg.replace(agg._1 + agg._2 + " as " + agg._3, agg._2 + " as " + agg._3)
      else
        queryWithoutAgg = queryWithoutAgg.replace(agg._1 + "(" + agg._2 + ")" + " as " + agg._3, agg._2 + " as " + agg._3)
    }


    // Remove group by, since we removed aggregates
    if (hasGroupBy)
      queryWithoutAgg = queryWithoutAgg.substring(0, queryWithoutAgg.indexOf("group"))

    val joinGraph = new JoinGraphV2()

    val pp = new PartitionPickerMultiTable(spark, joinGraph, dataDir, statBuilder)
    val joinPaths = pp.createPartitions(tableNames, exprs, innerConnector, outerConnector, predCols, joinInputs,
      groupingCols, isRandom)


    for ((table, index) <- tableNames.zipWithIndex){
      val partitions = getListOfFiles2(dataDir + table, excludedFiles)
      for (part <- partitions){
        val nType = if (index == 0) "start" else if (index == tableNames.size - 1) "end" else "interm"
        val vertex = new joinGraph.Node(part.getName, nType, table)
        joinGraph.addVertex(vertex)
      }

    }

    for (ji <- joinInputs)
      joinGraph.addEdges(ji._1, ji._2)

    val paths: ListBuffer[ListBuffer[Set[joinGraph.Node]]] = joinGraph.calculatePaths()

    var i = 0
    val maxIterations = math.min(10.0, joinPaths.size.toDouble).toInt
    // errors per iteration
    val errorsPerIter: ListBuffer[(Double, Double)] = ListBuffer()
    val foundGroups: ListBuffer[String] = ListBuffer()

    // total number of tuples in each table
    val statDir = "Statistics_" + dataDir.slice(dataDir.indexOf("sf"), dataDir.length)
    val totalTupleCounts: mutable.Map[String, Long] = Map()
    for (table <- tableNames)
      totalTupleCounts += (table -> statBuilder.getStatAsString(statDir + "/" + table + "/totalnumtuples.txt").toLong)

    // already sampled partitions
    val sampledPartitions: ListBuffer[String] = ListBuffer()

    // result cache, so keep track of all groups found so far
    var resCache: mutable.Map[(String, String), Map[String, String]] = Map()

    println(joinPaths.size)


    val seen: ListBuffer[String] = ListBuffer()

    for (path <- joinPaths) {
      for (node <- path._1.getNodes()) {
        val key = node.head.getTableName() + "/" + node.head.getName()
        seen.append(key)
      }
    }

    while (i < maxIterations) {
      val time = System.nanoTime
      val sampledPaths = pp.samplePaths(joinPaths, p.toInt)

      // For each table, load the list of its sampled partitions
      for ((table, index) <- tableNames.zipWithIndex){
        val partitionNames: ListBuffer[String] = ListBuffer()
        for (path <- sampledPaths){
          partitionNames += dataDir + table + "/" + path.getNodes()(index).head.getName()
        }
        val partitionDF = spark.read.parquet(partitionNames: _*)

        partitionDF.createOrReplaceTempView(table)
      }

      val partial = spark.sql(queryWithoutAgg)

      if (partial.count() > 0L) {
        // add sampled paths
        for (path <- sampledPaths) {
          for (node <- path.getNodes()) {
            val key = node.head.getTableName() + "/" + node.head.getName()
            sampledPartitions.append(key)
          }
        }

        /*
        Step 1: Join all tuples, without computing aggregate, so that we can compute their combined sid
         */
        // Assign sid's to result tuples
        val sidColumns: Array[String] = partial.schema.fieldNames.filter(col => col.contains("_sid"))
        var resultWithSid = partial.withColumn("sid", sidUDF(lit(b), struct(partial.columns map col: _*),
          array(sidColumns.map(lit(_)): _*)))

        // Result with newly assigned SIDs
        for (sid <- sidColumns) {
          resultWithSid = resultWithSid.drop(sid)
        }

        resultWithSid.createOrReplaceTempView("result")

        /*
        Step 2: Compute the aggregates on the joined tuples, group by sid
         */
        val groupings: Seq[String] = qp.parseQueryGrouping(logicalPlan)
        val cols: Seq[String] = resultWithSid.columns.toSeq
        var aggQueryWithSid = "select "

        // Hack to check if a column is an aggregate : check if it contains a bracket (
        for (col <- cols) {
          aggQueryWithSid = aggQueryWithSid + (if (aggregates.map(_._3).contains(col)) aggregates.find(_._3 == col).
            get._1 + "(" + (if (aggregates.find(_._3 == col).get._3 == "count_supplier") "distinct " else "") + col +
            ")" + " as " + col else col) + ","
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


        val sf = math.max(1.0, 1.0 / (sampledPartitions.toSet.size.toDouble / seen.size.toDouble))
        val partialResult = resultCombiner.combine(resultAggregatedWithSid, aggregatesNoSid, sf)
        // update cache
        if (i == 0) {
          resCache = partialResult.clone()
        } else {
          for ((k,v) <- partialResult){
            if (!resCache.contains(k))
              resCache += (k -> v)
            else
              resCache(k) = v
          }
        }

        var avgError = 0.0
        val trueNumGroups = exactResult.size.toDouble
        val numAggs = aggregates.size.toDouble

        // for each group
        for ((group, aggs) <- exactResult) {
          // for each aggregate in the group
          val groupString = group.mkString(",")
          //println(partialResult.keys.mkString("\n"))

          // check if group is missing
          if (resCache.contains((aggs.head._1, groupString))) {
            for (aggval <- aggs) {
              val aggName = aggval._1
              val aggTrueValue = aggval._2.toDouble

              val aggEstValue = resCache((aggName, groupString))("est").toDouble
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
        val duration = (System.nanoTime - time) / 1e9d
        println(avgError, missingGroupPct, duration)

      } else {
          errorsPerIter.append((1.0, 1.0))
      }

      i += 1

    } // end while

    errorsPerIter

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

    h.toInt.toString
  }


}
