package ProgQueries
import FileHandlers.TableDefs
import org.apache.spark.sql.functions._
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}
import scala.util.control.Breaks._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.{BufferedWriter, File, FileWriter}
import scala.language.postfixOps
import Query.{BenchQueries, QueryParser}
import scala.collection.mutable


object entryPoint {
  val excludedFiles = List("crc", "_SUCCESS")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark SQL basic example")
      //.config("spark.master", "spark://mcs-computeA002:7077")
      .config("spark.master", "local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val params = Map("errorTol" -> "2.0",
      "samplePercent" -> "10",
      "b" -> "50",
      "dataDir" -> "partitioned_with_sid_sf10/",
      "warehouseDir" -> "sample_warehouse",
      "alpha" -> "0.05")

    val query = BenchQueries.QueryTwoSid

    val qp = new QueryParser()
    val numTables = qp.getTables(spark, query).length


    if (numTables == 1)
      SingleTableQueryExecutor.runSingleTableQuery(spark, query, params, Map[String, Any](), "0")
    else
      MultiTableQueryExecutor.runMultiTableQuery(spark, query, params, Map[String, Any](), "0")


    //spark.read.option("header", true).csv("spark-warehouse/q1_sf10.csv").show()
    //runExactQuery(spark, query)


    println("Program finished")
   // System.in.read();
   // spark.stop();
  }



  // TODO: Fix scaling
  def calculateSF(sampleChoices: Map[String, (String, String)], i: Integer): Double ={

    var samplingFrac = 1.0
    var prevKey = ""

    for ((k,v) <- sampleChoices){
      if (v._1 == "unif"){
        samplingFrac *= (i.toDouble+1.0) / 100
      } else if (v._1 == "univ") {
        if (v._2 != prevKey)
          samplingFrac *= (i.toDouble + 1.0) / 100
      }

      prevKey = v._2
    }
      //math.pow((i.toDouble+1.0) / 100, tableNames.size.toDouble)
    //    val samplingFrac = ((i.toDouble+1.0) * 10.0) / 100 * 0.01
    val sf = 1.0 / samplingFrac
    return sf
  }


  def writeEstToFile(errors : ListBuffer[ListBuffer[(String, Double)]]): Unit ={
    val file = new File("q3unifEst.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    for (error <- errors) {
      bw.write(error.mkString(","))
      bw.write("\n")
    }
    bw.close()
  }


  def writeErrorsToFile(errors : ListBuffer[ListBuffer[(String, String, String, Double)]]): Unit ={
    val file = new File("spark-warehouse/errors.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    for (error <- errors) {
      bw.write(error.mkString(" "))
      bw.write("\n")
    }
    bw.close()
  }


  def runExactQuery(spark: SparkSession, query: String): Unit ={
    TableDefs.load_tpch_tables(spark, "data_parquet/")
    //spark.sql(query).coalesce(1).write.csv("spark-warehouse/query" + qID + ".csv")

    spark.sql(query).coalesce(1).write.option("header", "true").csv("q1_sf10" + ".csv")

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