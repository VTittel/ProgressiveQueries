package FileHandlers

import ProgQueries.Samplers
import ProgQueries.entryPoint.{excludedFiles, getListOfFiles, getListOfFiles2}
import com.twitter.algebird._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.approx_count_distinct
import org.apache.spark.sql.types._

import java.io._
import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.hashing.MurmurHash3

class StatisticsBuilder(sparkS: SparkSession, dir: String) {

  val Dir = dir
  val samplers = new Samplers
  var cmSketches: mutable.Map[String, TopCMS[Long]] = mutable.Map()
  val spark = sparkS
  val sf = dir.slice(dir.indexOf("sf"), dir.length)


  def calculatePartitionSize(tableName: String): Unit ={

    val partitionNames = getListOfFiles2(Dir + tableName, excludedFiles)

    for (partitionName <- partitionNames) {
      val partition = spark.read.parquet(partitionName.toString)

      val count = partition.count().toString

      val dirPath = "Statistics_" + sf + tableName + "/" + partitionName.getName + "/"
      val dir = new File(dirPath)
      dir.mkdirs()
      val file = new File(dirPath + "numtuples.txt")
      if (file.exists()) {
        writeStatToFile("numtuples", count, file, true)
      } else {
        writeStatToFile("numtuples", count, file, false)
      }

    }
  }


  def calculateTableSize(tableName: String): Unit ={

    val numTuples = spark.read.parquet(Dir + tableName).count().toString
    val dirPath = "Statistics_" + sf + "/" + tableName + "/"
    val dir = new File(dirPath)
    dir.mkdirs()
    val file = new File(dirPath + "totalnumtuples.txt")
    if (file.exists()) {
      writeStatToFile("numtuples", numTuples, file, true)
    } else {
      writeStatToFile("numtuples", numTuples, file, false)
    }

  }


  def calculateCardinality(tableName: String, colName: String): Unit ={

    val partitionNames = getListOfFiles2(Dir + tableName, excludedFiles)

    for (partitionName <- partitionNames) {
      val partition = spark.read.parquet(partitionName.toString)

      val distinctApprox = partition.agg(approx_count_distinct(colName)).rdd.map(r => r(0).toString).collect()
        .flatMap(s => scala.util.Try(s).toOption).toList.head.toDouble

      val cardinality = (distinctApprox / partition.count().toDouble).toString

      val dirPath = "Statistics_" + sf + tableName + "/" + partitionName.getName + "/" + colName + "/"
      val dir = new File(dirPath)
      dir.mkdirs()
      val file = new File(dirPath + "cardinality.txt")
      if (file.exists()) {
        writeStatToFile("cardinality", cardinality, file, true)
      } else {
        writeStatToFile("cardinality", cardinality, file, false)
      }

    }
  }


  def calculateHeavyHitters(tableName: String, colName: String, hhPct: Double): Unit ={

    val delta = 0.01
    val eps = 0.000000001
    val seed = 1
    val CMS_MONOID = TopPctCMS.monoid[Long](eps, delta, seed, hhPct)

    val partitionNames = getListOfFiles2(Dir + tableName, excludedFiles)

    for (partitionName <- partitionNames) {
      val partition =  if (tableName == "region" || tableName == "nation" || tableName == "supplier")
        spark.read.parquet(partitionName.toString) else spark.read.parquet(partitionName.toString).sample(0.075)
      val col = partition.select(colName).rdd.map(r => r(0).toString).collect()
        .flatMap(s => scala.util.Try(s).toOption).toList

      val data = col.map(x => MurmurHash3.stringHash(x).toLong)

      val cms = CMS_MONOID.create(data)

      val heavyHitters = cms.heavyHitters.map(x => x.toString)

      val dirPath = "Statistics_" + sf + tableName + "/" + partitionName.getName + "/" + colName + "/"
      val dir = new File(dirPath)
      dir.mkdirs()
      val file = new File(dirPath + "heavyhitters.txt")
      if (file.exists()) {
        writeListToFile(heavyHitters, file, true)
      } else {
        writeListToFile(heavyHitters, file, false)
      }

    }

  }


  def createHistogram(tableName: String, colName: String): Unit ={
    val partitionNames = getListOfFiles2(Dir + tableName, excludedFiles)

    for (partitionName <- partitionNames) {
      val partition =  if (tableName == "region" || tableName == "nation" || tableName == "supplier")
        spark.read.parquet(partitionName.toString) else spark.read.parquet(partitionName.toString).sample(0.075)
      val col = partition.select(colName).rdd.map(r => r(0).toString).collect()
        .flatMap(s => scala.util.Try(s).toOption).toList

      val d = math.ceil(col.length / 10.0).toInt

      val boundaries: ListBuffer[(String, String, String)] = ListBuffer()

      val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")

      val sortedCol = partition.schema(colName).dataType match {
        case _: DecimalType => col.map(x => x.toDouble).sorted.map(x => x.toString)
        case _: DateType => col.map(x => dateFormat.parse(x).getTime).sorted.map(x => x.toString)
        case _: IntegerType => col.map(x => x.toInt).sorted.map(x => x.toString)
        case _: LongType =>  col.map(x => x.toLong).sorted.map(x => x.toString)
        case _: StringType => col.map(x => MurmurHash3.stringHash(x).toLong).sorted.map(x => x.toString)
      }

      for (i <- sortedCol.indices by d){
        val temp = if (i + d > sortedCol.length) sortedCol.slice(i,sortedCol.length).take((i + d) - sortedCol.length)
        else sortedCol.slice(i, sortedCol.length).take(d)
        // boundaries
        val bLow = temp.head
        val bHigh = temp.last
        // unique count
        val distinct = temp.distinct.size
        boundaries += ((bLow, bHigh, distinct.toString))
      }

      val dirPath = "Statistics_" + sf + tableName + "/" + partitionName.getName + "/" + colName + "/"
      val dir = new File(dirPath)
      dir.mkdirs()
      val file = new File(dirPath + "histogram.txt")
      if (file.exists()) {
        writeHistogramToFile(boundaries, file, true)
      } else {
        writeHistogramToFile(boundaries, file, false)
      }

    }

  }


  def createCMS(tableName: String, colName: String, p: Double): Unit = {

    val DELTA = 0.01
    val EPS = 0.000000001
    val SEED = 1
    val HEAVY_HITTERS_PCT = 0.01
    val CMS_MONOID = TopPctCMS.monoid[Long](EPS, DELTA, SEED, HEAVY_HITTERS_PCT)

    val partitionNames = getListOfFiles2(Dir + tableName, excludedFiles)

    for (partitionName <- partitionNames) {
      val partition = if (tableName == "nation" || tableName == "region" || tableName == "supplier")
        spark.read.parquet(partitionName.toString) else spark.read.parquet(partitionName.toString).sample(p)

      val col = partition.select(colName).rdd.map(r => r(0).toString).collect()
        .flatMap(s => scala.util.Try(s).toOption).toList

      val data = col.map(x => MurmurHash3.stringHash(x).toLong)

      val cms = CMS_MONOID.create(data)
      val cmsPath = partitionName.toString + "/" + colName + "/cms"
      var temp = cmsPath.substring(cmsPath.indexOf("/")+1)
      temp = "Statistics_" + sf + temp
      cmSketches += (temp -> cms)
      /*
      val dirPath = "Statistics_" + sf + tableName + "/" + partitionName.getName + "/" + colName + "/"
      val dir = new File(dirPath)
      dir.mkdirs()
      val zipFile = new FileOutputStream(dirPath + "cms")
      val oos = new ObjectOutputStream(zipFile)
      oos.writeObject(cms)
      oos.close()

       */
    }

  }


  def loadCMS(cmsSchema: mutable.Map[String, List[String]]): Unit = {

    for ((k,v) <- cmsSchema) {
      for (key <- v) {
        val partitionNames = getListOfFiles(new File("Statistics_" + sf + k), excludedFiles)
        for (partitionName <- partitionNames) {
          val cmsPath = partitionName.toString + "/" + key + "/cms"
          val ois = new ObjectInputStream(new FileInputStream(cmsPath))
          val cms = ois.readObject.asInstanceOf[TopCMS[Long]]
          ois.close
          cmSketches += (cmsPath -> cms)
        }
      }

    }
  }


  def getCMS(table: String, partition: String, col: String): TopCMS[Long] = {
    val cmsPath = "Statistics_" + sf + table + "/" + partition + "/" + col + "/cms"
    cmSketches(cmsPath)
  }



  def writeHistogramToFile(hist: ListBuffer[(String, String, String)], filePath: File, append: Boolean){
    val fw = new FileWriter(filePath, append)
    for (bucket <- hist) {
      fw.write(bucket._1 + "," + bucket._2 + "," + bucket._3)
      fw.write(System.getProperty("line.separator"))
    }
    fw.close()
  }


  def writeStatToFile(statType: String, stat: String, filePath: File, append: Boolean): Unit = {
    val fw = new FileWriter(filePath, append)
    fw.write(statType + "," + stat)
    fw.write(System.getProperty("line.separator"))
    fw.close()
  }


  def writeListToFile(heavyHitters: Set[String], filePath: File, append: Boolean): Unit = {
    val fw = new FileWriter(filePath, append)
    for (hh <-heavyHitters){
      fw.write(hh)
      fw.write(System.getProperty("line.separator"))
    }
    fw.close()
  }


  def getStatAsString(path: String): String ={
    val bufferedSource = Source.fromFile(path)
    val line = bufferedSource.getLines().toList.head
    val splitLine = line.split(",").map(_.trim)
    val stat = splitLine(1)
    bufferedSource.close

    stat
  }


  def getHeavyHittersAsList(path: String): ListBuffer[String] ={
    val bufferedSource = Source.fromFile(path)
    val hhLB: ListBuffer[String] = ListBuffer()
    for (line <- bufferedSource.getLines) {
      hhLB.append(line)
    }
    bufferedSource.close

    hhLB
  }


  def getHistogram(path: String): ListBuffer[(Long, Long, Long)] ={
    val bufferedSource = Source.fromFile(path)
    val hhLB: ListBuffer[(Long, Long, Long)] = ListBuffer()
    for (line <- bufferedSource.getLines) {
      val splitLine = line.split(",").map(_.trim)
      hhLB.append((splitLine.head.toDouble.toLong, splitLine(1).toDouble.toLong, splitLine(2).toDouble.toLong))
    }
    bufferedSource.close

    hhLB
  }

}
