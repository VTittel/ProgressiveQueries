package Bench

import FileHandlers.StatisticsBuilder
import ProgQueries.{MultiTableQueryExecutor, SingleTableQueryExecutor}
import Query.BenchQueries
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map}

class RlBench {

  def runQuery1(spark: SparkSession, dataDir: String, sf: String): Unit ={
    val query = BenchQueries.Query1
    val dir = dataDir + "_sf" + sf + "/"

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

    val avgEstErrors: ListBuffer[Double] = ListBuffer()
    val avgGrpErrors: ListBuffer[Double] = ListBuffer()
    val estErrors: ListBuffer[List[Double]] = ListBuffer()
    val grpErrors: ListBuffer[List[Double]] = ListBuffer()
    val reruns = 1
    val iterations = 10
    val execTimes: ListBuffer[Double] = ListBuffer()


    for (i <- 0 until reruns){
      val time = System.nanoTime

      val errorsForIter: ListBuffer[(Double, Double)] = SingleTableQueryExecutorRl.runSingleTableQuery(spark, query, dir,
        exactResult)

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

    // table samplers
    // table -> (sampler, key)
    val samplers = Map("customer" -> ("universe", "c_custkey"),
    "order" -> ("universe", "o_custkey"),
    "lineitem" -> ("uniform", "l_orderkey"))

    for (i <- 0 until reruns){
      val time = System.nanoTime

      val errorsForIter = MultiTableQueryExecutorRl.runMultiTableQuery(spark, query, dir, exactResult, samplers)

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

    // table samplers
    // table -> (sampler, key)
    val samplers = Map("order" -> ("universe", "o_orderkey"),
      "lineitem" -> ("universe", "l_orderkey"))

    for (i <- 0 until reruns){
      val time = System.nanoTime

      val errorsForIter = MultiTableQueryExecutorRl.runMultiTableQuery(spark, query, dir, exactResult, samplers)

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

    // table samplers
    // table -> (sampler, key)
    val samplers = Map("customer" -> ("universe", "c_custkey"),
      "order" -> ("universe", "o_custkey"),
      "lineitem" -> ("uniform", ""),
      "supplier" -> ("uniform", ""),
      "nation" -> ("uniform", ""),
      "region" -> ("uniform", ""))

    for (i <- 0 until reruns){
      val time = System.nanoTime

      val errorsForIter = MultiTableQueryExecutorRl.runMultiTableQuery(spark, query, dir, exactResult, samplers)

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

    // table samplers
    // table -> (sampler, key)
    val samplers = Map("customer" -> ("universe", "c_custkey"),
      "order" -> ("universe", "o_custkey"),
      "lineitem" -> ("uniform", ""),
      "nation" -> ("uniform", ""))

    for (i <- 0 until reruns){
      val time = System.nanoTime

      val errorsForIter = MultiTableQueryExecutorRl.runMultiTableQuery(spark, query, dir, exactResult, samplers)


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


    // table samplers
    // table -> (sampler, key)
    val samplers = Map("order" -> ("universe", "o_orderkey"),
      "lineitem" -> ("universe", "l_orderkey"))

    for (i <- 0 until reruns){
      val time = System.nanoTime

      val errorsForIter = MultiTableQueryExecutorRl.runMultiTableQuery(spark, query, dir, exactResult, samplers)

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


    // table samplers
    // table -> (sampler, key)
    val samplers = Map("partsupp" -> ("universe", "ps_partkey"),
      "part" -> ("universe", "p_partkey"))

    for (i <- 0 until reruns){
      val time = System.nanoTime

      val errorsForIter = MultiTableQueryExecutorRl.runMultiTableQuery(spark, query, dir, exactResult, samplers)

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

    // table samplers
    // table -> (sampler, key)
    val samplers = Map("lineitem" -> ("universe", "l_partkey"),
      "part" -> ("universe", "p_partkey"))

    for (i <- 0 until reruns){
      val time = System.nanoTime

      val errorsForIter = MultiTableQueryExecutorRl.runMultiTableQuery(spark, query, dir, exactResult, samplers)

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
    val iterations = 1
    val execTimes: ListBuffer[Double] = ListBuffer()

    // table samplers
    // table -> (sampler, key)
    val samplers = Map("order" -> ("universe", "o_orderkey"),
      "lineitem" -> ("universe", "l_orderkey"),
      "supplier" -> ("uniform", ""),
      "nation" -> ("uniform", ""))

    for (i <- 0 until reruns){
      val time = System.nanoTime

      val errorsForIter = MultiTableQueryExecutorRl.runMultiTableQuery(spark, query, dir, exactResult, samplers)

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

}
