package ProgQueries

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import scala.collection._

object testClass {

  def main (args: Array[String]): Unit = {
  
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
       .config("spark.master", "local")
      .getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")
    
    val numPart = 10.0
    
    val dataSplit = Array.fill(numPart.toInt)(1/numPart)
    
    val nums = Array(1,2,3,4,5,5,5,5,5,5,5,5,5,5,5,5,5,6,6,7,7,7,7,8,8,8,9,9)
    val numseq: Seq[(Int, Int)] = nums.zipWithIndex
    val numsDF = spark.sparkContext.parallelize(numseq)
      .toDF("value", "index");
   
    
    val arr = numsDF.randomSplit(dataSplit)
    
    val testDF = arr(1)
    
    testDF.show()
    
  }
    
  
}