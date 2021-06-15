package ProgQueries

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.util.zip.CRC32
import org.apache.spark.sql.functions._

import java.math.BigInteger
import java.security.MessageDigest
import scala.util.hashing.{MurmurHash3 => MH3}


class Samplers {


  def testSampler(table: DataFrame, joinKey: String): DataFrame = {

    // Column hashing function
    val hash = udf((key: String) => {
      /*
      val crc = new CRC32
      crc.update(key.getBytes)

      (crc.getValue / Math.pow(2, 32).toFloat).toString()

       */
      key.hashCode.toString

   //   key.map(_.toByte.toInt).sum.toString
    })

    val hashColName = joinKey + "_hash"
    val hashedTable = table.withColumn(hashColName, hash(col(joinKey)))
    return hashedTable
    //return hashedTable.filter(hashColName + " < " + p.toString)
  }


  def hash1(table: DataFrame, joinKey: String): DataFrame = {

    val hash = udf((key: String) => {
      val md = MessageDigest.getInstance("MD5")
      val digest = md.digest(key.getBytes).slice(0,4)
      val hash = new BigInteger(1,digest).doubleValue()
      hash / math.pow(2, 32)
    })

    val hashColName = joinKey + "_hash"
    val hashedTable = table.withColumn(hashColName, hash(col(joinKey)))
    hashedTable

  }


  def hash2(table: DataFrame, joinKey: String): DataFrame = {

    val hash = udf((key: String) => {
      val md = MessageDigest.getInstance("MD5")
      val digest = md.digest(key.getBytes).slice(4,8)
      val hashval = new BigInteger(1,digest).doubleValue()
      hashval / math.pow(2, 32)
    })

    val hashColName = joinKey + "_hash"
    val hashedTable = table.withColumn(hashColName, hash(col(joinKey)))
    hashedTable

  }



  def multHashSampler(table: DataFrame, joinKey: String, A: Double): DataFrame = {

    val hash = udf((key: String) => {
      val hashVal = math.pow(2,64) * (((key.hashCode & 0xfffffff).toDouble * A) % 1)
      hashVal / math.pow(2,64)
    })

    val hashColName = joinKey + "_hash"
    val hashedTable = table.withColumn(hashColName, hash(col(joinKey)))
    hashedTable

  }


  def stratifiedSampler(table: DataFrame, key: String, p: Double): DataFrame = {
   // val keys = List("c_nationkey", "c_mktsegment")
    /*
    var fractions = table.select(keys.head, keys.tail: _*).distinct()
      .withColumn("fraction", lit(p))
      .rdd
      .map { case Row(k, l, fraction: Double) => (k.toString + l.toString, fraction) }
      .collectAsMap().toMap

    val fractions2 = fractions.asInstanceOf[Map[Any, Double]]

     */
    val fractions = table.select(key).distinct()
      .withColumn("fraction", lit(p))
      .rdd
      .map { case Row(k, fraction: Double) => (k, fraction) }
      .collectAsMap().toMap

//    return table.rdd.keyBy(x=>x(0)).sampleByKey(false, fractions2)
    return table.stat.sampleBy(key, fractions, 2L)
  }


  def universeSampler(table: DataFrame, joinKey: String): DataFrame = {

    // Column hashing function
    val hash = udf((key: String) => {
      val crc = new CRC32
      crc.update(key.getBytes)

      (crc.getValue / Math.pow(2, 32).toFloat).toString()
    })

    val hashColName = joinKey + "_hash"
    val hashedTable = table.withColumn(hashColName, hash(col(joinKey)))
    hashedTable
  }


  def universeSamplerV2(table: DataFrame, joinKey: String, p: Double): DataFrame = {

    // Column hashing function
    val hash = udf((key: String) => {
      val crc = new CRC32
      crc.update(key.getBytes)

      (crc.getValue / Math.pow(2, 32).toFloat).toString()
    })

    val hashColName = joinKey + "_hash"
    val hashedTable = table.withColumn(hashColName, hash(col(joinKey)))
    hashedTable.filter(hashColName + " < " + p.toString)
  }


  def uniformSampler(table: DataFrame, end: Integer): DataFrame = {
    val start = 1
    val rnd = new scala.util.Random
    start + rnd.nextInt( (end - start) + 1 )

    val assign_sample_group = udf(() => {
      start + rnd.nextInt( (end - start) + 1 )
    })

    val sampledTable = table.withColumn("unif_sample_group", assign_sample_group())
    return sampledTable
  }


  def rangeSampler(table: DataFrame, rangeCol: String, p0: Integer, p1: Integer): DataFrame = {

    val end = 100
    val range = udf((hash: String) => {
      val hashDouble = hash.toDouble
      var retValue = 0

      for (i <- 1 until end){
        if (hashDouble >= (i / end.toDouble) && hashDouble < ((i+1) / end.toDouble))
          retValue = i
      }

      retValue.toString
    })


    val rangeTable = table.withColumn("univ_partition", range(col(rangeCol)))

    return rangeTable.filter(("univ_partition" + " >= " + p0.toString + " and "
      + "univ_partition" + " < " + p1.toString))
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