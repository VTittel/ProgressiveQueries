package Bench


import FileHandlers.StatisticsBuilder
import PartitionLogic.{JoinGraphV2, PartitionPickerMultiTable}
import ProgQueries.{ResultCombiner, Samplers}
import ProgQueries.entryPoint.{excludedFiles, getListOfFiles2, sample}
import Query.{BenchQueries, QueryParser}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{array, col, lit, struct, udf}

import java.io.File
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}


object MultiTableQueryExecutorRl {

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

  def runMultiTableQuery(spark: SparkSession, query: String, dataDir: String,
                         exactResult: Map[List[String], List[(String, String)]],
                        // table -> (sampler, key)
                         tableSamplers: Map[String, (String, String)]):  ListBuffer[(Double, Double)] = {


    // Keep track of error at each iteration
    val b = 100
    val qp = new QueryParser()
    val resultCombiner = new ResultCombiner()


    val tableNames = qp.getTables(spark, query)
    var p = 0.01

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
      // special distinct case
      if (agg._3 == "count_supplier"){
        queryWithoutAgg = queryWithoutAgg.replace("count(distinct(ps_suppkey)) as count_supplier",
          "ps_suppkey as count_supplier")
      }
      if (agg._2.head.toString == "(" && agg._2.last.toString == ")")
        queryWithoutAgg = queryWithoutAgg.replace(agg._1 + agg._2 + " as " + agg._3, agg._2 + " as " + agg._3)
      else
        queryWithoutAgg = queryWithoutAgg.replace(agg._1 + "(" + agg._2 + ")" + " as " + agg._3, agg._2 + " as " + agg._3)
    }


    // Remove group by, since we removed aggregates
    if (hasGroupBy)
      queryWithoutAgg = queryWithoutAgg.substring(0, queryWithoutAgg.indexOf("group"))


    var i = 0
    val maxIterations = 1
    // errors per iteration
    val errorsPerIter: ListBuffer[(Double, Double)] = ListBuffer()
    val foundGroups: ListBuffer[String] = ListBuffer()


    // result cache, so keep track of all groups found so far
    var resCache: mutable.Map[(String, String), Map[String, String]] = Map()


    while (i < maxIterations) {
      // For each table, load the list of its sampled partitions
      for ((table, sampler) <- tableSamplers){
        if (table == "supplier" || table == "nation" || table == "region"){
          val sample = spark.read.parquet(dataDir + table)
          sample.createOrReplaceTempView(table)
        } else {
            if (sampler._1 == "uniform") {
              val sample = spark.read.parquet(dataDir + table).sample(p)
              sample.write.mode(SaveMode.Overwrite).format("parquet").save("samples_" + dataDir + table)
              val sample2 = spark.read.parquet("samples_" + dataDir + table)
              sample2.createOrReplaceTempView(table)
            } else if (sampler._1 == "universe"){
              val data = spark.read.parquet(dataDir + table)
              val samplers = new Samplers
              val sample = samplers.universeSamplerV2(data, sampler._2, p)
              sample.write.mode(SaveMode.Overwrite).format("parquet").save("samples_" + dataDir + table)
              val sample2 = spark.read.parquet("samples_" + dataDir + table)
              sample2.createOrReplaceTempView(table)
            }
        }
      }

      val partial = spark.sql(queryWithoutAgg)

      if (partial.count() > 0L) {

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


        var sf = 1.0
        var prevSamp = ""

        for ((table, sampler) <- tableSamplers) {
          if (!(table == "supplier" || table == "nation" || table == "region")) {
            if (sampler._1 == "uniform") {
              sf *= (i.toDouble + 1.0) / 100.0
            } else if (sampler._1 == "universe") {
              if (prevSamp == "uniform")
                sf *= (i.toDouble + 1.0) / 100.0
            }
            prevSamp = sampler._1
          }
        }

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

        i += 1
      } else {
          errorsPerIter.append((1.0, 1.0))
          i += 1
      }

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
