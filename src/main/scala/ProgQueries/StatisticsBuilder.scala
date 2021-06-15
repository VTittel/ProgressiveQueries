package ProgQueries

import ProgQueries.entryPoint.{excludedFiles, getListOfFiles, getListOfFiles2}
import org.apache.spark.sql.SparkSession
import ProgQueries.MathFunctions._

import java.io.{File, FileInputStream, FileOutputStream, FileWriter, ObjectInputStream, ObjectOutputStream}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import com.twitter.algebird._
import scala.util.hashing.MurmurHash3

import CMSHasherImplicits._

class StatisticsBuilder(spark: SparkSession) {

  val Dir = "data_parquet/"
  val samplers = new Samplers
  var cmSketches: mutable.Map[String, TopCMS[Long]] = mutable.Map()


  def calculateMeasure(tableName: String, col: String, measure: String): Unit ={

    val partitionNames = getListOfFiles2(Dir + tableName, excludedFiles)

    for (partitionName <- partitionNames) {
      var partition = spark.read.parquet(partitionName.toString)
      var hashes = List.empty[Double]

      if (measure == "mean" || measure == "stdError") {
        partition = samplers.testSampler(partition, col)
        hashes = partition.select(col+"_hash").rdd.map(r => r(0).toString).collect()
          .flatMap(s => scala.util.Try(s.toDouble).toOption).toList
      }

      // Choose measure to calculate based on the measure parameter
      val measureVal = measure match {
        case "mean" => {
          mean(hashes)
        }
        case "stdError" => {
          stdError(hashes)
        }
        case "countUnique" => countUnique(partition, col)
      }

      val dirPath = "Statistics/" + tableName + "/" + partitionName.getName + "/" + col + "/"
      val dir = new File(dirPath)
      dir.mkdirs()
      val file = new File(dirPath + "statistics.txt")
      if (file.exists()) {
        writeStatToFile(measure, measureVal, file, true)
      } else {
        writeStatToFile(measure, measureVal, file, false)
      }
    }
  }


  def createCMS(spark: SparkSession, tableName: String, colName: String): Unit = {

    val DELTA = 0.01
    val EPS = 0.00001
    val SEED = 1
    val HEAVY_HITTERS_PCT = 0.01
    val p = 0.01

    val partitionNames = getListOfFiles2(Dir + tableName, excludedFiles)

    for (partitionName <- partitionNames) {
      val partition = spark.read.parquet(partitionName.toString).sample(p)
      val col = partition.select(colName).rdd.map(r => r(0).toString).collect()
        .flatMap(s => scala.util.Try(s).toOption).toList

      val CMS_MONOID = TopPctCMS.monoid[Long](EPS, DELTA, SEED, HEAVY_HITTERS_PCT)

      val data = col.map(x => MurmurHash3.stringHash(x).toLong)

      val cms = CMS_MONOID.create(data)

      val dirPath = "Statistics/" + tableName + "/" + partitionName.getName + "/" + colName + "/"
      val dir = new File(dirPath)
      dir.mkdirs()
      val zipFile = new GZIPOutputStream(new FileOutputStream(dirPath + "cms"))
      val oos = new ObjectOutputStream(zipFile)
      oos.writeObject(cms)
      oos.close()
    }

  }


  def loadCMS(): Unit = {
    val cmsSchema = Map(
      "customer" -> List("c_custkey"),
      "order" -> List("o_custkey", "o_orderkey"),
      "lineitem" -> List("l_orderkey"))


    for ((k,v) <- cmsSchema) {
      for (key <- v) {
        val partitionNames = getListOfFiles(new File("Statistics/" + k), excludedFiles)
        for (partitionName <- partitionNames) {
          val cmsPath = partitionName.toString + "/" + key + "/cms"
          val ois = new ObjectInputStream(new GZIPInputStream(new FileInputStream(cmsPath)))
          val cms = ois.readObject.asInstanceOf[TopCMS[Long]]
          ois.close
          cmSketches += (cmsPath -> cms)
        }
      }

    }
  }


  def getCMS(table: String, partition: String, col: String): TopCMS[Long] = {
    val cmsPath = "Statistics/" + table + "/" + partition + "/" + col + "/cms"
    cmSketches(cmsPath)
  }


  def writeStatToFile(statType: String, stat: String, filePath: File, append: Boolean): Unit = {
    val fw = new FileWriter(filePath, append)
    fw.write(statType + "," + stat)
    fw.write(System.getProperty("line.separator"))
    fw.close()
  }


  def getStatsAsMap(path: String): mutable.Map[String, String] ={
    val bufferedSource = Source.fromFile(path)
    val statsMap = mutable.Map[String, String]()
    for (line <- bufferedSource.getLines) {
      val splitLine = line.split(",").map(_.trim)
      statsMap += (splitLine.head -> splitLine(1))
    }
    bufferedSource.close

    statsMap
  }

}
