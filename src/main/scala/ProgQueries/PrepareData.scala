package ProgQueries

import ProgQueries.entryPoint.{excludedFiles, getListOfFiles, sortWithSampleNr}
import org.apache.spark.sql.SparkSession
import java.io.File
import scala.reflect.io.Directory

class PrepareData {

  def prepare(spark: SparkSession, path: String, nOfSamples: Integer, keepOldDir: Boolean): Unit = {
    partitionBySampleGroup(spark, path, nOfSamples)
    generateDataWithSid(spark, "partitioned_sf150", keepOldDir)
  }


  /*
  Function to break a table into x number of uniform samples. Each sample is stored in its own file.

  @param spark, the current spark instance
  @param dir, the original file directory
  @param nOfSamples, number of sample groups into which the original table should be divided
   */
  def partitionBySampleGroup(spark: SparkSession, dir: String, nOfSamples: Integer): Unit = {
    val S = new Samplers()
    val tableDirs = getListOfFiles(new File(dir), excludedFiles)

    for (tableDir <- tableDirs) {
      var table = spark.read.parquet(tableDir.toString)
      table = S.assign_uniform_samples(table, nOfSamples)
      table.write.partitionBy("unif_sample_group").parquet("partitioned_sf150/" + tableDir.getName)
    }
  }


  /*
  Function to assign SIDs to each table.
  Due to how parquet works, adding a column requires to reading the original data, adding the sid column,
  and writing the data back to disk in a different directory.

  @param spark, the current Spark session
  @param dir, the original file directory
  @param keepOldDir, boolean to indicate whether the original directory should be kept or delete
  @return nothing
   */
  def generateDataWithSid(spark: SparkSession, dir: String, keepOldDir: Boolean): Unit ={

    val Eval = new Evaluation()
    val tableDirs = getListOfFiles(new File(dir), excludedFiles)

    for (tableDir <- tableDirs) {
      // Sort the directories based on uniform_sample_group, for convenience
      val tableSubDirs = sortWithSampleNr(getListOfFiles(new File(tableDir.toString), excludedFiles))

      for (tableSubDir <- tableSubDirs) {
        var table = spark.read.parquet(tableSubDir.toString)
        table = Eval.assignSubsamples(spark, table, tableDir.getName, table.count(), 100)
        table.write.mode("overwrite").parquet("partitioned_with_sid_sf150/" + tableDir.getName + "/" + tableSubDir.getName)
      }
    }

    if (!keepOldDir) {
      val directory = new Directory(new File(dir))
      directory.deleteRecursively()
    }

  }

}
