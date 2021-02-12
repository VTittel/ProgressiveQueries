package ProgQueries
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}


class Experimental {
  def DoStuff(spark: SparkSession): Unit ={

    val startTime = System.nanoTime
    val lineitemRDD = spark.read.parquet("partitioned_with_sid_sf10/lineitem.parquet/unif_sample_group=1").rdd
    val orderRDD = spark.read.parquet("partitioned_with_sid_sf10/order.parquet/unif_sample_group=1").rdd
    val lineitemPairRDD = lineitemRDD.map(row => (row.getAs("l_orderkey").toString,
      row.getAs("l_orderkey").toString))
    val orderPairRdd = orderRDD.map(row => (row.getAs("o_orderkey").toString,
      row.getAs("o_totalprice").toString))

    val coGroupedRDD = lineitemPairRDD.groupWith(orderPairRdd)
    println(coGroupedRDD.collect().take(5).mkString("Array(", ", ", ")"))

    val endTime = System.nanoTime
    val elapsedMs = (endTime - startTime) / 1e6d
    println("Time taken : " + elapsedMs + "ms")
   // test.take(1).foreach(println)



  }

}
