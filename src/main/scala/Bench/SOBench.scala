package Bench

import org.apache.spark.sql.SparkSession

class SOBench {

  def prepareData(spark: SparkSession): Unit ={
    import com.databricks.spark.xml._

    val dir = "so_raw/"
    val tables = List("badges.xml", "comments.xml", "posthistory.xml", "posts.xml", "users.xml", "votes.xml")

    for (table <- tables){
      val df = spark.read
        .option("rootTag", "posts")
        .option("rowTag", "row")
        .xml(dir + table)

      df.write.format("parquet").save("so_parquet/" + table)
    }

  }


  def coalesceTables(spark: SparkSession): Unit ={
    val dir = "so_raw/"
    val tables = List("badges.xml", "comments.xml", "posthistory.xml", "posts.xml", "users.xml", "votes.xml")

    /*
    for (table <- tables) {
      spark.read.parquet(dataDir + "_sf" + sf + "/" + table._1).coalesce(table._2).write.format("parquet")
        .save(dataDir + "_sf" + sf + "_temp/" + table._1)

    }

     */
  }

}
