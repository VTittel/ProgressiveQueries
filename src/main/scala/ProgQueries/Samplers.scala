package ProgQueries

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import java.util.zip.CRC32
import org.apache.spark.sql.functions._

class Samplers {


  def stratifiedSampler(table: DataFrame, key: String, p: Double): DataFrame = {
    val fractions = table.select(key).distinct()
      .withColumn("fraction", lit(p))
      .rdd
      .map { case Row(k, fraction: Double) => (k.toString.toInt, fraction) }
      .collectAsMap().toMap

    return table.stat.sampleBy(key, fractions, 2L)
  }


  def universeSampler(table: DataFrame, joinKey: String): DataFrame = {

    // Column hashing function
    val hash = udf((key: String) => {
      val crc = new CRC32
      crc.update(key.getBytes);

      (crc.getValue / Math.pow(2, 32).toFloat).toString()
    })

    val hashColName = joinKey + "_hash"
    val hashedTable = table.withColumn(hashColName, hash(col(joinKey)))
    return hashedTable
    //return hashedTable.filter(hashColName + " < " + p.toString)
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

}