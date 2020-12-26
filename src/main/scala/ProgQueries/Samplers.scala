package ProgQueries

import org.apache.spark.sql.DataFrame
import java.util.zip.CRC32
import org.apache.spark.sql.functions._
import scala.reflect.api.materializeTypeTag

class Samplers {
  /*
  TODO
  def stratified_sampling(table: DataFrame): DataFrame = {
    val data: RDD[(Int, Row)] = table.rdd.keyBy(_.getInt(0))
    val fractions: Map[Int, Double] = table.map(_._1)
                                      .distinct
                                      .map(x => (x, 0.8))
                                      .collectAsMap

  }

  */

  def universe_sampling(table: DataFrame, join_key: String): DataFrame = {

    // Column hashing function
    val hash = udf((key: String) => {
      val crc = new CRC32
      crc.update(key.getBytes);

      (crc.getValue / Math.pow(2, 32).toFloat).toString()
    })

    val hashColName = join_key + "_hash"
    val hashedTable = table.withColumn(hashColName, hash(col(join_key)))
    return hashedTable

    //return hashedTable.filter(hashColName + " < " + p.toString)
  }


  def uniform_sampling(table: DataFrame, p: Double): DataFrame = {
    val r = scala.util.Random

    val incl = udf(() => {
      val rng = r.nextDouble()

      if (rng <= p)
        "1";
      else
        "0";
    })

    val sampledTable = table.withColumn("", incl())
    return sampledTable
  }


  def assign_uniform_samples(table: DataFrame, end: Integer): DataFrame = {
    val start = 1
    val rnd = new scala.util.Random
    start + rnd.nextInt( (end - start) + 1 )

    val assign_sample_group = udf(() => {
      start + rnd.nextInt( (end - start) + 1 )
    })

    val sampledTable = table.withColumn("unif_sample_group", assign_sample_group())
    return sampledTable
  }


  def bucketBy(table: DataFrame, join_key: String): DataFrame = {

    val assign_bucket = udf((key: String) => {
      val crc = new CRC32
      crc.update(key.getBytes)
      val hash = (crc.getValue)
      val bucket_nr = Math.floorMod(hash, 10)

      bucket_nr.toString

    })

    val bucketedTable = table.withColumn("bucket_nr", assign_bucket(col(join_key)))
    return bucketedTable
  }


  def range_sampling(table: DataFrame, join_key: String, p: Double): DataFrame = {

    val range: Double => Int = x => x match {
      case x if (x >= 0.0 && x < 0.1) => 1
      case x if (x >= 0.1 && x < 0.2) => 2
      case x if (x >= 0.2 && x < 0.3) => 3
      case x if (x >= 0.3 && x < 0.4) => 4
      case x if (x >= 0.4 && x < 0.5) => 5
      case x if (x >= 0.5 && x < 0.6) => 6
      case x if (x >= 0.6 && x < 0.7) => 7
      case x if (x >= 0.7 && x < 0.8) => 8
      case x if (x >= 0.8 && x < 0.9) => 9
      case x if (x >= 0.9 && x < 1.0) => 10

    }

    val groupUDF = udf(range)

    var rangeTable = table.withColumn("Group", groupUDF(col("c_custkey_hash")))

    val x = 0.99

    val ranges = Map(
      1 -> x,
      2 -> x,
      3 -> x,
      4 -> x,
      5 -> x,
      6 -> x,
      7 -> x,
      8 -> x,
      9 -> x,
      10 -> x)

    rangeTable = rangeTable.stat.sampleBy("Group", ranges, seed = 11L)

    return rangeTable
  }

}