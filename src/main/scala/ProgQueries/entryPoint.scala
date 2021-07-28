package ProgQueries
import FileHandlers.{CreateTPCHparquet, StatisticsBuilder, TableDefs}

import scala.collection.mutable.{ListBuffer, Map}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.{BufferedWriter, File, FileWriter}
import scala.language.postfixOps
import Query.{BenchQueries, QueryParser}
import Bench.SOBench
import Server.SingleTableQueryExecutorTest

import scala.collection.mutable


object entryPoint {
  val excludedFiles = List("crc", "_SUCCESS")


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark SQL basic example")
     // .config("spark.master", "spark://mcs-computeA004:7077")
      .config("spark.master", "local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val query = BenchQueries.Query1
    val qp = new QueryParser()
    val numTables = qp.getTables(spark, query).length
    val dataDir = "data_parquet"

    //calculateStats(spark, dataDir, "1")
    runQuery4(spark, dataDir, 0.05, "1", isRandom = false)


    /*
    if (numTables == 1)
      SingleTableQueryExecutor.runSingleTableQuery(spark, query, dataDir, errorTol, Map[String, Any](), "0")
    else
      MultiTableQueryExecutor.runMultiTableQuery(spark, query, dataDir, errorTol, Map[String, Any](), "0")

     */


    println("Program finished")
   // System.in.read();
   // spark.stop();
  }



  def runQuery1(spark: SparkSession, dataDir: String, errorTol: Double, sf: String, isRandom: Boolean): Unit ={

    val query = BenchQueries.Query1
    val dir = dataDir + "_sf" + sf + "/"

    // Stats
    val statBuilder = new StatisticsBuilder(spark, dir)


    //calculate exact query
    val exactResult = Map[List[String], List[(String, String)]]()
    val full = spark.read.option("header", true).csv("results_full/sf_" + sf + "/query_1.csv")
    full.collect().foreach(row =>
      exactResult += List(row.getAs("l_returnflag").toString, row.getAs("l_linestatus").toString) ->
        List(("sum_qty", row.getAs[String]("sum_qty")),
          ("sum_base_price", row.getAs[String]("sum_base_price")),
          ("sum_disc_price", row.getAs[String]("sum_disc_price")),
          ("avg_qty", row.getAs[String]("avg_qty")),
          ("avg_price", row.getAs[String]("avg_price")),
          ("avg_disc", row.getAs[String]("avg_disc")))
    )

    //where clause
    val exprs = List(List(("l_shipdate", "<", "1998-12-01", "DateType")))
    val innerConnector = "none"
    val outerConnector = "none"
    val predColsSingle = List("l_shipdate")

    val avgEstErrors: ListBuffer[Double] = ListBuffer()
    val avgGrpErrors: ListBuffer[Double] = ListBuffer()
    val estErrors: ListBuffer[List[Double]] = ListBuffer()
    val grpErrors: ListBuffer[List[Double]] = ListBuffer()
    val reruns = 1
    val iterations = 10
    val execTimes: ListBuffer[Double] = ListBuffer()


    for (i <- 0 until reruns){
      val time = System.nanoTime

      val errorsForIter: ListBuffer[(Double, Double)] = SingleTableQueryExecutor.runSingleTableQuery(spark, query, dir,
        errorTol, exactResult, statBuilder, Map[String, Any](), "0", exprs, innerConnector, outerConnector,
        predColsSingle, isRandom)

      val duration = (System.nanoTime - time) / 1e9d
      execTimes.append(duration)

      val estErrorsForIter = errorsForIter.map(x => x._1).toList
      val grpErrorsForIter = errorsForIter.map(x => x._2).toList
      estErrors.append(estErrorsForIter)
      grpErrors.append(grpErrorsForIter)
    }

    for (i <- 0 until iterations) {
      var avgEst = 0.0
      var avgGrp = 0.0
      for (j <- 0 until reruns) {
        avgEst += estErrors(j)(i)
        avgGrp += grpErrors(j)(i)
      }
      avgEst = avgEst / reruns.toDouble
      avgGrp = avgGrp / reruns.toDouble
      avgEstErrors.append(avgEst)
      avgGrpErrors.append(avgGrp)
    }

    var avgTime = 0.0
    for(time <- execTimes)
      avgTime += time

    avgTime = avgTime / reruns.toDouble

    println("SCALE FACTOR " + sf)
    println(avgEstErrors.mkString(","))
    println(avgGrpErrors.mkString(","))
    println(avgTime)

  }


  def runQuery3(spark: SparkSession, dataDir: String, errorTol: Double, sf: String, isRandom: Boolean): Unit ={

    val query = BenchQueries.Query3
    val dir = dataDir + "_sf" + sf + "/"
    val statBuilder = new StatisticsBuilder(spark, dir)

    //where clause
    val exprs = Map("lineitem" -> List(List(("l_shipdate", ">", "1995-03-17", "DateType"))),
      "order" -> List(List(("o_orderdate", "<", "1995-03-15", "DateType"))),
      "customer"-> List(List(("c_mktsegment", "=", "FURNITURE", "StringType"))))

    val innerConnector = "none"
    val outerConnector = "and"
    val predColsMulti = mutable.Map("order" -> List("o_orderdate"),
                                    "customer" -> List("c_mktsegment"),
                                    "lineitem" -> List("l_shipdate"))

    val groupingColsMulti = mutable.Map("order" -> List("o_orderdate", "o_shippriority"))

    // cms schema
    val cmsSchema = Map(
      "customer" -> List("c_custkey"),
      "order" -> List("o_custkey", "o_orderkey"),
      "lineitem" -> List("l_orderkey"))

    // cm sketches
    for ((k,v) <- cmsSchema){
      val table = k
      val cols = v
      for (col <- cols) {
        statBuilder.createCMS(table, col, 0.01) // change to 0.01 for live
      }
    }

    val exactResult = Map[List[String], List[(String, String)]]()
    val full = spark.read.option("header", true).csv("results_full/sf_" + sf + "/query_3.csv")
    full.collect().foreach(row =>
      exactResult += List(row.getAs("o_shippriority").toString, row.getAs("o_orderdate").toString) ->
        List(("sum_revenue", row.getAs[String]("sum_revenue")))
    )

    val avgEstErrors: ListBuffer[Double] = ListBuffer()
    val avgGrpErrors: ListBuffer[Double] = ListBuffer()
    val estErrors: ListBuffer[List[Double]] = ListBuffer()
    val grpErrors: ListBuffer[List[Double]] = ListBuffer()
    val reruns = 1
    val iterations = 10
    val execTimes: ListBuffer[Double] = ListBuffer()


    for (i <- 0 until reruns){
      val time = System.nanoTime

      val errorsForIter = MultiTableQueryExecutor.runMultiTableQuery(spark, query, dir, errorTol, exactResult,
        statBuilder, Map[String, Any](), "0", exprs, innerConnector, outerConnector, predColsMulti,
        groupingColsMulti, isRandom)


      val duration = (System.nanoTime - time) / 1e9d
      execTimes.append(duration)

      val estErrorsForIter = errorsForIter.map(x => x._1).toList
      val grpErrorsForIter = errorsForIter.map(x => x._2).toList
      estErrors.append(estErrorsForIter)
      grpErrors.append(grpErrorsForIter)
    }


    for (i <- 0 until iterations) {
      var avgEst = 0.0
      var avgGrp = 0.0
      for (j <- 0 until reruns) {
        avgEst += estErrors(j)(i)
        avgGrp += grpErrors(j)(i)
      }
      avgEst = avgEst / reruns.toDouble
      avgGrp = avgGrp / reruns.toDouble
      avgEstErrors.append(avgEst)
      avgGrpErrors.append(avgGrp)
    }

    var avgTime = 0.0
    for(time <- execTimes)
      avgTime += time

    avgTime = avgTime / reruns.toDouble

    println("SCALE FACTOR " + sf)
    println(avgEstErrors.mkString(","))
    println(avgGrpErrors.mkString(","))
    println(avgTime)

  }


  def runQuery4(spark: SparkSession, dataDir: String, errorTol: Double, sf: String, isRandom: Boolean): Unit ={

    val query = BenchQueries.Query4
    val dir = dataDir + "_sf" + sf + "/"
    val statBuilder = new StatisticsBuilder(spark, dir)

    //where clause
    val exprs = Map("lineitem" -> List(List(("l_commitdate", "<", "1996-01-06", "DateType"))))

    val innerConnector = "none"
    val outerConnector = "none"
    val predColsMulti = mutable.Map("lineitem" -> List("l_commitdate"))
    val groupingColsMulti = mutable.Map("order" -> List("o_orderpriority"))

    // cms schema
    val cmsSchema = Map("order" -> List("o_orderkey"),
      "lineitem" -> List("l_orderkey"))

    // cm sketches
    for ((k,v) <- cmsSchema){
      val table = k
      val cols = v
      for (col <- cols) {
        statBuilder.createCMS(table, col, 0.01) // change to 0.01 for live
      }
    }


    val exactResult = Map[List[String], List[(String, String)]]()
    val full = spark.read.option("header", true).csv("results_full/sf_" + sf + "/query_4.csv")
    full.collect().foreach(row =>
      exactResult += List(row.getAs("o_orderpriority").toString) ->
        List(("count_orders", row.getAs[String]("count_orders")))
    )


    val avgEstErrors: ListBuffer[Double] = ListBuffer()
    val avgGrpErrors: ListBuffer[Double] = ListBuffer()
    val estErrors: ListBuffer[List[Double]] = ListBuffer()
    val grpErrors: ListBuffer[List[Double]] = ListBuffer()
    val reruns = 1
    val iterations = 10
    val execTimes: ListBuffer[Double] = ListBuffer()


    for (i <- 0 until reruns){
      val time = System.nanoTime

      val errorsForIter = MultiTableQueryExecutor.runMultiTableQuery(spark, query, dir, errorTol, exactResult,
        statBuilder, Map[String, Any](), "0", exprs, innerConnector, outerConnector, predColsMulti,
        groupingColsMulti, isRandom)

      val duration = (System.nanoTime - time) / 1e9d
      execTimes.append(duration)

      val estErrorsForIter = errorsForIter.map(x => x._1).toList
      val grpErrorsForIter = errorsForIter.map(x => x._2).toList
      estErrors.append(estErrorsForIter)
      grpErrors.append(grpErrorsForIter)
    }


    for (i <- 0 until iterations) {
      var avgEst = 0.0
      var avgGrp = 0.0
      for (j <- 0 until reruns) {
        avgEst += estErrors(j)(i)
        avgGrp += grpErrors(j)(i)
      }
      avgEst = avgEst / reruns.toDouble
      avgGrp = avgGrp / reruns.toDouble
      avgEstErrors.append(avgEst)
      avgGrpErrors.append(avgGrp)
    }

    var avgTime = 0.0
    for(time <- execTimes)
      avgTime += time

    avgTime = avgTime / reruns.toDouble

    println("SCALE FACTOR " + sf)
    println(avgEstErrors.mkString(","))
    println(avgGrpErrors.mkString(","))
    println(avgTime)

  }


  def runQuery5(spark: SparkSession, dataDir: String, errorTol: Double, sf: String, isRandom: Boolean): Unit ={

    val query = BenchQueries.Query5
    val dir = dataDir + "_sf" + sf + "/"
    val statBuilder = new StatisticsBuilder(spark, dir)

    //where clause
    val exprs = Map("order" -> List(List(("o_orderdate", "<", "1994-01-01", "DateType"))))

    val innerConnector = "none"
    val outerConnector = "and"
    val predColsMulti = mutable.Map("order" -> List("o_orderdate"))
    val groupingColsMulti = mutable.Map("nation" -> List("n_name"))

    val cmsSchema: Map[String, List[String]] = Map(
      "lineitem" -> List("l_orderkey", "l_suppkey"),
      "order" -> List("o_orderkey", "o_custkey"),
      "customer" -> List("c_custkey"),
      "supplier" -> List("s_suppkey", "s_nationkey"),
      "nation" -> List("n_nationkey", "n_regionkey"),
      "region" -> List("r_regionkey"))

    // cm sketches
    for ((k,v) <- cmsSchema){
      val table = k
      val cols = v
      for (col <- cols) {
        statBuilder.createCMS(table, col, 0.01) // change to 0.01 for live
      }
    }


    val exactResult = Map[List[String], List[(String, String)]]()
    val full = spark.read.option("header", true).csv("results_full/sf_" + sf + "/query_5.csv")
    full.collect().foreach(row =>
      exactResult += List(row.getAs("n_name").toString) ->
        List(("sum_revenue", row.getAs[String]("sum_revenue")))
    )


    val avgEstErrors: ListBuffer[Double] = ListBuffer()
    val avgGrpErrors: ListBuffer[Double] = ListBuffer()
    val estErrors: ListBuffer[List[Double]] = ListBuffer()
    val grpErrors: ListBuffer[List[Double]] = ListBuffer()
    val reruns = 1
    val iterations = 10
    val execTimes: ListBuffer[Double] = ListBuffer()


    for (i <- 0 until reruns){
      val time = System.nanoTime

      val errorsForIter = MultiTableQueryExecutor.runMultiTableQuery(spark, query, dir, errorTol, exactResult,
        statBuilder, Map[String, Any](), "0", exprs, innerConnector, outerConnector, predColsMulti,
        groupingColsMulti, isRandom)

      val duration = (System.nanoTime - time) / 1e9d
      execTimes.append(duration)

      val estErrorsForIter = errorsForIter.map(x => x._1).toList
      val grpErrorsForIter = errorsForIter.map(x => x._2).toList
      estErrors.append(estErrorsForIter)
      grpErrors.append(grpErrorsForIter)
    }


    for (i <- 0 until iterations) {
      var avgEst = 0.0
      var avgGrp = 0.0
      for (j <- 0 until reruns) {
        avgEst += estErrors(j)(i)
        avgGrp += grpErrors(j)(i)
      }
      avgEst = avgEst / reruns.toDouble
      avgGrp = avgGrp / reruns.toDouble
      avgEstErrors.append(avgEst)
      avgGrpErrors.append(avgGrp)
    }

    var avgTime = 0.0
    for(time <- execTimes)
      avgTime += time

    avgTime = avgTime / reruns.toDouble

    println("SCALE FACTOR " + sf)
    println(avgEstErrors.mkString(","))
    println(avgGrpErrors.mkString(","))
    println(avgTime)
  }


  def runQuery10(spark: SparkSession, dataDir: String, errorTol: Double, sf: String, isRandom: Boolean): Unit ={

    val query = BenchQueries.Query10
    val dir = dataDir + "_sf" + sf + "/"
    val statBuilder = new StatisticsBuilder(spark, dir)

    //where clause
    val exprs = Map("order" -> List(List(("o_orderdate", ">", "1993-10-01", "DateType"))),
      "lineitem" -> List(List(("l_returnflag", "=", "R", "StringType"))))

    val innerConnector = "none"
    val outerConnector = "and"
    val predColsMulti = mutable.Map("lineitem" -> List("l_returnflag"), "order" -> List("o_orderdate"))
    val groupingColsMulti = mutable.Map("nation" -> List("n_name"), "customer" -> List("c_nationkey"),
      "lineitem" -> List("l_shipmode"))

    val cmsSchema: Map[String, List[String]] = Map(
      "lineitem" -> List("l_orderkey"),
      "order" -> List("o_orderkey", "o_custkey"),
      "customer" -> List("c_custkey", "c_nationkey"),
      "nation" -> List("n_nationkey"))

    // cm sketches
    for ((k,v) <- cmsSchema){
      val table = k
      val cols = v
      for (col <- cols) {
        statBuilder.createCMS(table, col, 0.01) // change to 0.01 for live
      }
    }

    val exactResult = Map[List[String], List[(String, String)]]()
    val full = spark.read.option("header", true).csv("results_full/sf_" + sf + "/query_10.csv")
    full.collect().foreach(row =>
      exactResult += List(row.getAs("l_shipmode").toString, row.getAs("c_nationkey").toString,
        row.getAs("n_name").toString) -> List(("sum_revenue", row.getAs[String]("sum_revenue")))
    )


    val avgEstErrors: ListBuffer[Double] = ListBuffer()
    val avgGrpErrors: ListBuffer[Double] = ListBuffer()
    val estErrors: ListBuffer[List[Double]] = ListBuffer()
    val grpErrors: ListBuffer[List[Double]] = ListBuffer()
    val reruns = 1
    val iterations = 10
    val execTimes: ListBuffer[Double] = ListBuffer()


    for (i <- 0 until reruns){
      val time = System.nanoTime

      val errorsForIter = MultiTableQueryExecutor.runMultiTableQuery(spark, query, dir, errorTol, exactResult,
        statBuilder, Map[String, Any](), "0", exprs, innerConnector, outerConnector, predColsMulti,
        groupingColsMulti, isRandom)

      val duration = (System.nanoTime - time) / 1e9d
      execTimes.append(duration)

      val estErrorsForIter = errorsForIter.map(x => x._1).toList
      val grpErrorsForIter = errorsForIter.map(x => x._2).toList
      estErrors.append(estErrorsForIter)
      grpErrors.append(grpErrorsForIter)
    }


    for (i <- 0 until iterations) {
      var avgEst = 0.0
      var avgGrp = 0.0
      for (j <- 0 until reruns) {
        avgEst += estErrors(j)(i)
        avgGrp += grpErrors(j)(i)
      }
      avgEst = avgEst / reruns.toDouble
      avgGrp = avgGrp / reruns.toDouble
      avgEstErrors.append(avgEst)
      avgGrpErrors.append(avgGrp)
    }

    var avgTime = 0.0
    for(time <- execTimes)
      avgTime += time

    avgTime = avgTime / reruns.toDouble

    println("SCALE FACTOR " + sf)
    println(avgEstErrors.mkString(","))
    println(avgGrpErrors.mkString(","))
    println(avgTime)
  }


  def runQuery12(spark: SparkSession, dataDir: String, errorTol: Double, sf: String, isRandom: Boolean): Unit ={

    val query = BenchQueries.Query12
    val dir = dataDir + "_sf" + sf + "/"
    val statBuilder = new StatisticsBuilder(spark, dir)

    //where clause
    val exprs = Map("lineitem" -> List(List(("l_receiptdate", ">", "1994-01-01", "DateType"))))

    val innerConnector = "none"
    val outerConnector = "none"
    val predColsMulti = mutable.Map("lineitem" -> List("l_receiptdate"))
    val groupingColsMulti = mutable.Map("lineitem" -> List("l_shipmode"))

    val cmsSchema: Map[String, List[String]] = Map(
      "lineitem" -> List("l_orderkey"),
      "order" -> List("o_orderkey"))

    // cm sketches
    for ((k,v) <- cmsSchema){
      val table = k
      val cols = v
      for (col <- cols) {
        statBuilder.createCMS(table, col, 0.01) // change to 0.01 for live
      }
    }


    val exactResult = Map[List[String], List[(String, String)]]()
    val full = spark.read.option("header", true).csv("results_full/sf_" + sf + "/query_12.csv")
    full.collect().foreach(row => exactResult += List(row.getAs("l_shipmode").toString) ->
      List(("sum_price", row.getAs[String]("sum_price")),
        ("avg_price", row.getAs[String]("avg_price")))
    )

    val avgEstErrors: ListBuffer[Double] = ListBuffer()
    val avgGrpErrors: ListBuffer[Double] = ListBuffer()
    val estErrors: ListBuffer[List[Double]] = ListBuffer()
    val grpErrors: ListBuffer[List[Double]] = ListBuffer()
    val reruns = 1
    val iterations = 10
    val execTimes: ListBuffer[Double] = ListBuffer()


    for (i <- 0 until reruns){
      val time = System.nanoTime

      val errorsForIter = MultiTableQueryExecutor.runMultiTableQuery(spark, query, dir, errorTol, exactResult,
        statBuilder, Map[String, Any](), "0", exprs, innerConnector, outerConnector, predColsMulti,
        groupingColsMulti, isRandom)

      val duration = (System.nanoTime - time) / 1e9d
      execTimes.append(duration)

      val estErrorsForIter = errorsForIter.map(x => x._1).toList
      val grpErrorsForIter = errorsForIter.map(x => x._2).toList
      estErrors.append(estErrorsForIter)
      grpErrors.append(grpErrorsForIter)
    }


    for (i <- 0 until iterations) {
      var avgEst = 0.0
      var avgGrp = 0.0
      for (j <- 0 until reruns) {
        avgEst += estErrors(j)(i)
        avgGrp += grpErrors(j)(i)
      }
      avgEst = avgEst / reruns.toDouble
      avgGrp = avgGrp / reruns.toDouble
      avgEstErrors.append(avgEst)
      avgGrpErrors.append(avgGrp)
    }

    var avgTime = 0.0
    for(time <- execTimes)
      avgTime += time

    avgTime = avgTime / reruns.toDouble

    println("SCALE FACTOR " + sf)
    println(avgEstErrors.mkString(","))
    println(avgGrpErrors.mkString(","))
    println(avgTime)
  }


  def runQuery16(spark: SparkSession, dataDir: String, errorTol: Double, sf: String, isRandom: Boolean): Unit ={

    val query = BenchQueries.Query16
    val dir = dataDir + "_sf" + sf + "/"
    val statBuilder = new StatisticsBuilder(spark, dir)

    //where clause
    val exprs = Map("partsupp" -> List(List(("ps_availqty", ">", "1000", "LongType")),
      List(("ps_availqty", "<", "1200", "LongType"))))

    val innerConnector = "none"
    val outerConnector = "and"
    val predColsMulti = mutable.Map("partsupp" -> List("ps_availqty"))
    val groupingColsMulti = mutable.Map("part" -> List("p_size"))

    val cmsSchema: Map[String, List[String]] = Map(
      "partsupp" -> List("ps_partkey"),
      "part" -> List("p_partkey"))

    // cm sketches
    for ((k,v) <- cmsSchema){
      val table = k
      val cols = v
      for (col <- cols) {
        statBuilder.createCMS(table, col, 0.01)
      }
    }


    val exactResult = Map[List[String], List[(String, String)]]()
    val full = spark.read.option("header", true).csv("results_full/sf_" + sf + "/query_16.csv")
    full.collect().foreach(row =>
      exactResult += List(row.getAs("p_type").toString)->
      List(("count_supplier", row.getAs[String]("count_supplier")),
        ("sum_retail_price", row.getAs[String]("sum_retail_price"))))

    val avgEstErrors: ListBuffer[Double] = ListBuffer()
    val avgGrpErrors: ListBuffer[Double] = ListBuffer()
    val estErrors: ListBuffer[List[Double]] = ListBuffer()
    val grpErrors: ListBuffer[List[Double]] = ListBuffer()
    val reruns = 1
    val iterations = 10
    val execTimes: ListBuffer[Double] = ListBuffer()


    for (i <- 0 until reruns){
      val time = System.nanoTime

      val errorsForIter = MultiTableQueryExecutor.runMultiTableQuery(spark, query, dir, errorTol, exactResult,
        statBuilder, Map[String, Any](), "0", exprs, innerConnector, outerConnector, predColsMulti,
        groupingColsMulti, isRandom)

      val duration = (System.nanoTime - time) / 1e9d
      execTimes.append(duration)

      val estErrorsForIter = errorsForIter.map(x => x._1).toList
      val grpErrorsForIter = errorsForIter.map(x => x._2).toList
      estErrors.append(estErrorsForIter)
      grpErrors.append(grpErrorsForIter)
    }


    for (i <- 0 until iterations) {
      var avgEst = 0.0
      var avgGrp = 0.0
      for (j <- 0 until reruns) {
        avgEst += estErrors(j)(i)
        avgGrp += grpErrors(j)(i)
      }
      avgEst = avgEst / reruns.toDouble
      avgGrp = avgGrp / reruns.toDouble
      avgEstErrors.append(avgEst)
      avgGrpErrors.append(avgGrp)
    }

    var avgTime = 0.0
    for(time <- execTimes)
      avgTime += time

    avgTime = avgTime / reruns.toDouble

    println("SCALE FACTOR " + sf)
    println(avgEstErrors.mkString(","))
    println(avgGrpErrors.mkString(","))
    println(avgTime)
  }


  def runQuery19(spark: SparkSession, dataDir: String, errorTol: Double, sf: String, isRandom: Boolean): Unit ={

    val query = BenchQueries.Query19
    val dir = dataDir + "_sf" + sf + "/"
    val statBuilder = new StatisticsBuilder(spark, dir)

    //where clause
    val exprs = Map("part" -> List(List(("p_brand", "=", "Brand#12", "StringType")),
      List(("p_container", "=", "MED BAG", "StringType"))),
      "lineitem" -> List(List(("l_quantity", ">", "10", "LongType"))))

    val innerConnector = "none"
    val outerConnector = "or"
    val predColsMulti = mutable.Map("part" -> List("p_brand", "p_container"), "lineitem" -> List("l_quantity"))
    val groupingColsMulti = mutable.Map("lineitem" -> List("l_linenumber"))

    val cmsSchema: Map[String, List[String]] = Map(
      "lineitem" -> List("l_partkey"),
      "part" -> List("p_partkey"))

    // cm sketches
    for ((k,v) <- cmsSchema){
      val table = k
      val cols = v
      for (col <- cols) {
        statBuilder.createCMS(table, col, 0.01) // change to 0.01 for live
      }
    }


    val exactResult = Map[List[String], List[(String, String)]]()
    val full = spark.read.option("header", true).csv("results_full/sf_" + sf + "/query_19.csv")
    full.collect().foreach(row => exactResult += List(row.getAs("l_linenumber").toString) ->
      List(("sum_revenue", row.getAs[String]("sum_revenue")),
        ("avg_price", row.getAs[String]("avg_price"))))

    val avgEstErrors: ListBuffer[Double] = ListBuffer()
    val avgGrpErrors: ListBuffer[Double] = ListBuffer()
    val estErrors: ListBuffer[List[Double]] = ListBuffer()
    val grpErrors: ListBuffer[List[Double]] = ListBuffer()
    val reruns = 1
    var iterations = 0
    val execTimes: ListBuffer[Double] = ListBuffer()


    for (i <- 0 until reruns){
      val time = System.nanoTime

      val errorsForIter = MultiTableQueryExecutor.runMultiTableQuery(spark, query, dir, errorTol, exactResult,
        statBuilder, Map[String, Any](), "0", exprs, innerConnector, outerConnector, predColsMulti,
        groupingColsMulti, isRandom)

      iterations = errorsForIter.size

      val duration = (System.nanoTime - time) / 1e9d
      execTimes.append(duration)

      val estErrorsForIter = errorsForIter.map(x => x._1).toList
      val grpErrorsForIter = errorsForIter.map(x => x._2).toList
      estErrors.append(estErrorsForIter)
      grpErrors.append(grpErrorsForIter)
    }


    for (i <- 0 until iterations) {
      var avgEst = 0.0
      var avgGrp = 0.0
      for (j <- 0 until reruns) {
        avgEst += estErrors(j)(i)
        avgGrp += grpErrors(j)(i)
      }
      avgEst = avgEst / reruns.toDouble
      avgGrp = avgGrp / reruns.toDouble
      avgEstErrors.append(avgEst)
      avgGrpErrors.append(avgGrp)
    }

    var avgTime = 0.0
    for(time <- execTimes)
      avgTime += time

    avgTime = avgTime / reruns.toDouble

    println("SCALE FACTOR " + sf)
    println(avgEstErrors.mkString(","))
    println(avgGrpErrors.mkString(","))
    println(avgTime)
  }


  def runQuery21(spark: SparkSession, dataDir: String, errorTol: Double, sf: String, isRandom: Boolean): Unit ={

    val query = BenchQueries.Query21
    val dir = dataDir + "_sf" + sf + "/"
    val statBuilder = new StatisticsBuilder(spark, dir)


    //where clause
    val exprs = Map("nation" -> List(List(("n_name", "=", "SAUDI ARABIA", "StringType")),
      List(("n_name", "=", "AMERICA", "StringType")),
      List(("n_name", "=", "BRAZIL", "StringType"))))

    val innerConnector = "none"
    val outerConnector = "or"
    val predColsMulti = mutable.Map("nation" -> List("n_name"))
    val groupingColsMulti = mutable.Map("supplier" -> List("s_name"))

    val cmsSchema: Map[String, List[String]] = Map(
      "lineitem" -> List("l_orderkey", "l_suppkey"),
      "order" -> List("o_orderkey"),
      "supplier" -> List("s_suppkey", "s_nationkey"),
      "nation" -> List("n_nationkey"))

    // cm sketches
    for ((k,v) <- cmsSchema){
      val table = k
      val cols = v
      for (col <- cols) {
        statBuilder.createCMS(table, col, 0.01) // change to 0.01 for live
      }
    }


    val exactResult = Map[List[String], List[(String, String)]]()
    val full = spark.read.option("header", true).csv("results_full/sf_" + sf + "/query_21.csv")
    full.collect().foreach(row => exactResult += List(row.getAs("s_name").toString) ->
      List(("avg_disc", row.getAs[String]("avg_disc")),
        ("count_tuples", row.getAs[String]("count_tuples"))))

    val avgEstErrors: ListBuffer[Double] = ListBuffer()
    val avgGrpErrors: ListBuffer[Double] = ListBuffer()
    val estErrors: ListBuffer[List[Double]] = ListBuffer()
    val grpErrors: ListBuffer[List[Double]] = ListBuffer()
    val reruns = 1
    val iterations = 10
    val execTimes: ListBuffer[Double] = ListBuffer()


    for (i <- 0 until reruns){
      val time = System.nanoTime

      val errorsForIter = MultiTableQueryExecutor.runMultiTableQuery(spark, query, dir, errorTol, exactResult,
        statBuilder, Map[String, Any](), "0", exprs, innerConnector, outerConnector, predColsMulti,
        groupingColsMulti, isRandom)

      val duration = (System.nanoTime - time) / 1e9d
      execTimes.append(duration)

      val estErrorsForIter = errorsForIter.map(x => x._1).toList
      val grpErrorsForIter = errorsForIter.map(x => x._2).toList
      estErrors.append(estErrorsForIter)
      grpErrors.append(grpErrorsForIter)
    }


    for (i <- 0 until iterations) {
      var avgEst = 0.0
      var avgGrp = 0.0
      for (j <- 0 until reruns) {
        avgEst += estErrors(j)(i)
        avgGrp += grpErrors(j)(i)
      }
      avgEst = avgEst / reruns.toDouble
      avgGrp = avgGrp / reruns.toDouble
      avgEstErrors.append(avgEst)
      avgGrpErrors.append(avgGrp)
    }

    var avgTime = 0.0
    for(time <- execTimes)
      avgTime += time

    avgTime = avgTime / reruns.toDouble

    println("SCALE FACTOR " + sf)
    println(avgEstErrors.mkString(","))
    println(avgGrpErrors.mkString(","))
    println(avgTime)

  }


  def calculateStats(spark: SparkSession, dataDir: String, sf: String): Unit = {

    val dir = dataDir + "_sf" + sf + "/"
    val statBuilder = new StatisticsBuilder(spark, dir)

    val groupingCols: Map[String, List[String]] = Map(
      "customer" -> List("c_nationkey"),
      "order" -> List("o_orderdate", "o_shippriority", "o_orderpriority"),
      "lineitem" -> List("l_returnflag", "l_linestatus", "l_shipmode", "l_linenumber"),
      "nation" -> List("n_name"),
      "supplier" -> List("s_name"),
      "part" -> List("p_brand", "p_type", "p_size"))

    val predCols: Map[String, List[String]] = Map(
      "customer" -> List("c_mktsegment"),
      "order" -> List("o_orderdate"),
      "lineitem" -> List("l_shipdate", "l_commitdate", "l_returnflag", "l_receiptdate", "l_quantity"),
      "nation" -> List("n_name"),
      "part" -> List("p_brand", "p_container", "p_size"),
      "region" -> List("r_name"))

    val joinCols: Map[String, List[String]] = Map(
      "lineitem" -> List("l_orderkey", "l_suppkey", "l_partkey"),
      "order" -> List("o_orderkey", "o_custkey"),
      "customer" -> List("c_custkey", "c_nationkey"),
      "supplier" -> List("s_suppkey", "s_nationkey"),
      "nation" -> List("n_nationkey", "n_regionkey"),
      "region" -> List("r_regionkey"),
      "part" -> List("p_partkey"),
      "partsupp" -> List("ps_partkey"))

    val tables = List("lineitem", "order", "customer", "supplier", "nation", "region", "part", "partsupp")


    for ((k,v) <- predCols){
      val table = k
      val cols = v
      for (col <- cols) {
        println(k,col)
        statBuilder.createHistogram(table, col)
      }
    }

    // grouping cols
    for ((k,v) <- groupingCols){
      val table = k
      val cols = v
      for (col <- cols) {
        println(k,col)
        statBuilder.calculateCardinality(table, col)
        statBuilder.calculateHeavyHitters(table, col, 0.01)
      }
    }

    println("Done grouping cols")

    // cm sketches
    for ((k,v) <- joinCols){
      val table = k
      val cols = v
      for (col <- cols) {
        println(table, col)
        statBuilder.createCMS(table, col, 0.1) // change to 0.01 for live
      }
    }

    println("Done sketches")

    // table and part sizes
    for (table <- tables){
      statBuilder.calculatePartitionSize(table)
      statBuilder.calculateTableSize(table)
    }

    println("Done sizes")
  }


  def coalescePartitions(spark: SparkSession, dataDir: String, sf: String): Unit = {
    // val tables = List(("lineitem", 100), ("order", 45), ("customer", 10), ("part", 4),  ("partsupp", 50))
    //val tables = List(("lineitem", 30), ("order", 15), ("customer", 4), ("part", 1),  ("partsupp", 15))
    val tables = List(("supplier", 1))
    for (table <- tables) {
      spark.read.parquet(dataDir + "_sf" + sf + "/" + table._1).coalesce(table._2).write.format("parquet")
        .save(dataDir + "_sf" + sf + "_temp/" + table._1)

    }
  }


  def runExactQuery(spark: SparkSession, query: String, sf: String, queryNum: String): Unit ={
    TableDefs.load_tpch_tables(spark, "data_parquet_sf" + sf + "/")
    val outputPath = "results_full/" + "sf_" + sf + "/query_" + queryNum + ".csv"
    spark.sql(query).coalesce(1).write.option("header", "true").csv(outputPath)

  }


  def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isDirectory).toList.filter { file =>
      !extensions.exists(file.getName.endsWith(_  ))
    }
  }

  def getListOfFiles2(dir: String, extensions: List[String]):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList.filter { file =>
        !extensions.exists(file.getName.endsWith(_  ))
      }
    } else {
      List[File]()
    }
  }


  final def sample[A](dist: scala.collection.mutable.Map[A, Double]): A = {
    val p = scala.util.Random.nextDouble * dist.values.sum
    val it = dist.iterator
    var accum = 0.0
    while (it.hasNext) {
      val (item, itemProb) = it.next
      accum += itemProb
      if (accum >= p)
        return item // return so that we don't have to search through the whole distribution
    }
    sys.error(f"Sampling error") // needed so it will compile
  }


}