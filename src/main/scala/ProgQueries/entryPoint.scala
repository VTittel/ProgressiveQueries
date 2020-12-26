package ProgQueries
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.functions._

import scala.collection.mutable.Map
import scala.util.control.Breaks._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types._

import scala.collection.mutable
import java.io.File
import scala.reflect.io.Directory


object entryPoint {
  val excludedFiles = List("crc", "_SUCCESS")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local[4]")
      .config("spark.executor.core", "4")
      //.config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.sources.bucketing.enabled", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val params = Map("errorTol" -> "0.5",
      "sampleSize" -> "10",
      "dataDir" -> "partitioned_with_sid_sf10/",
      "alpha" -> "0.05")

    val agg = ("avg", "o_totalprice")
    val join_inputs = Array(
      ("lineitem", "orders", "orderkey"))

    val query1 = """select avg(o_totalprice) from lineitem
      join order on lineitem.l_orderkey = orders.o_orderkey
      """

    val query2 = """select avg(o_totalprice), o_orderstatus from order
      where o_orderpriority = '5-LOW'
      group by o_orderstatus
      """


    // True answer - 188869.469330
    // True answer query2 - 151069.216228
  //  TableDefs.load_tpch_tables(spark: SparkSession, "data_parquet_sf10/": String)
  //  spark.sql(query2).show()
    run_simple_query(spark, query2, params, agg)
    //run_join_query(spark, join_inputs, agg)
  //  partitionBySampleGroup(spark, "data_parquet_sf10", 100)
 //   generate_data_with_sid(spark, "partitioned_sf10", false)
    println("Program finished");
 //   System.in.read();
  //  spark.stop();
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
      table.write.partitionBy("unif_sample_group").parquet("partitioned_sf10/" + tableDir.getName)
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
  def generate_data_with_sid(spark: SparkSession, dir: String, keepOldDir: Boolean): Unit ={

    val Eval = new Evaluation()
    val tableDirs = getListOfFiles(new File(dir), excludedFiles)

    for (tableDir <- tableDirs) {
      // Sort the directories based on uniform_sample_group, for convenience
      val tableSubDirs = sortWith_sample_nr(getListOfFiles(new File(tableDir.toString), excludedFiles))

      for (tableSubDir <- tableSubDirs) {
        var table = spark.read.parquet(tableSubDir.toString)
        table = Eval.assign_subsamples(spark, table, tableDir.getName, table.count(), 100)
        table.write.mode("overwrite").parquet("partitioned_with_sid_sf10/" + tableDir.getName + "/" + tableSubDir.getName)
      }
    }

    if (!keepOldDir) {
      val directory = new Directory(new File("delete_test"))
      directory.deleteRecursively()
    }

  }


  def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isDirectory).toList.filter { file =>
      !extensions.exists(file.getName.endsWith(_  ))
    }
  }


  /* Sort files containing samples */
  def sortWith_sample_nr(files: List[File]): List[File] ={
    val extractor = "(?<=\\=).*".r

    return files.sortWith({ (l: File, r: File) =>
      val lnumber = extractor findFirstMatchIn  l.getName
      val rnumber = extractor findFirstMatchIn  r.getName

      lnumber.mkString.toInt < rnumber.mkString.toInt
    })

  }

  /*
  Method to execute a query on a single table progressively. Single aggregate functions only
  @param spark, the current spark Session
  @param query, String of sql query
  @param params: query and evaluation parameters
  @param agg: type of aggregate function and column its applied on
  @return nothing
   */

  def run_simple_query(spark: SparkSession, query: String, params: Map[String, String], agg: (String, String)): Unit ={
    // Sample size, expressed in % of total table size. Only discrete values between 0-100 are possible for now
    val maxIterations: Integer = params("sampleSize").toInt
    val topDir = params("dataDir")

    // Error tolerance, i.e. when to stop
    val errorTol = params("errorTol").toDouble

    // Keep track of error at each iteration
    var currentError = errorTol + 1
    var i: Integer = 0

    var result = spark.emptyDataFrame

    val Eval = new Evaluation()

    // Get names of all subdirectories in tableName.parquet dir
    val tableName = getTables(spark, query).head
    val files = sortWith_sample_nr(getListOfFiles(new File(topDir + tableName + ".parquet"), excludedFiles))
    // Query rewrite depends if the query has a group by clause
    val hasGroupBy:Boolean = query.contains("group by")

    while (i < maxIterations) {
      /*
      Rewrite the query to directly load the parquet file from the query, and add group by sid.
      */
      var newQuery = ""

      if (hasGroupBy)
        newQuery = query.replace(" " + tableName, " parquet.`" + files(i) + "`") + "," +
          tableName.take(1) + "_sid"
      else
        newQuery = query.replace(" " + tableName, " parquet.`" + files(i) + "`") +
        "group by " + tableName.take(1) + "_sid"


      val partial = spark.sql(newQuery)

      if ( i == 0)
        result = partial
      else
        result = result.union(partial)


      result.createOrReplaceTempView("result")

      val cols: Seq[String] = result.columns.toSeq.drop(1)
      val colsString: String = cols.mkString(",")
      var aggQuery = ""

      if (hasGroupBy)
        aggQuery = "select " + agg._1 + "(`" +  agg._1 + "(" + agg._2 + ")`) " + " as `" +
          agg._1 + "(" + agg._2 + ")`, " + colsString + " from result group by " + colsString

      else
        aggQuery = "select " + colsString  + agg._1 + "(`" +  agg._1 + "(" + agg._2 + ")`) " + " as `" +
          agg._1 + "(" + agg._2 + ")`" + " from result"


      val resultAggregated = spark.sql(aggQuery)


      // Evaluate new result
      val res = Eval.eval_single_table(result, params, agg, i+1)
      currentError = res("error").toDouble

      println("Result : ")
      resultAggregated.show()
      println("CI : " + "[" + res("ci_low") + " , " + res("ci_high") + "]")
      println("Error : " + currentError)

      // Break if accuracy is achieved
      if (currentError <= errorTol)
        break

      i += 1
    }

  }


/*
  def run_join_query(spark: SparkSession, join_inputs: Array[(String, String, String)], agg: (String, String)){

    // Number of tables
    val ntables = join_inputs.length
    val npart = 10.0
    val b: Integer = 100

    // All results computed so far
    var result = spark.emptyDataFrame
    val tol = 5.0

    // Keep track of error at each iteration
    var currentError = tol + 1
    var num_iter = 1
    val total_iter = math.pow(npart, ntables + 1)
    // Keep track of join partitions
    var indices = Array.fill(ntables + 1)(1)

    val Eval = new Evaluation()

    val query = """select avg(o_totalprice) from lineitem
      join orders on lineitem.l_orderkey = orders.o_orderkey
      """

    val pr = query indexOf "from"
    println(query.substring(pr, query.length))

    // Load the data as temp views
    val tables = getTables(spark, query)

    for (table <- tables){
      var tableDF = spark.read.parquet("partitioned/" + table)
      tableDF = Eval.assign_subsamples(spark, tableDF, table, tableDF.count(), b)
      tableDF.createOrReplaceTempView(table)
    }

    val startTime = System.nanoTime

    while ((num_iter <= 1)) {
      // Modify query and do the join for current batch
      // Bucket index, which bucket we should use
      var i_part = 0
      var mod_query = "select " + agg._2 +  " from "
      for (join_pair <- join_inputs){

        mod_query = mod_query + join_pair._1 + " join " + join_pair._2 + " on " + join_pair._1 +
          "." + join_pair._1.take(1) + "_" + join_pair._3 + "=" + join_pair._2 + "."  +
          join_pair._2.take(1) + "_" + join_pair._3 + " where " + join_pair._1 + ".bucket_nr=" +
          indices(i_part) + " and " + join_pair._2 + ".bucket_nr=" + indices(i_part+1)

        i_part += 1
      }
      val partial = spark.sql(mod_query)
     // partial.show()

      // Add new samples to old ones
      if ( num_iter == 1)
        result = partial
      else
        result = result.union(partial)

      // Evaluate new result
      /*
      var res = eval(result, join_inputs, agg, spark: SparkSession)
      currentError = res._1
      val est = res._2

      println("Result : " + est)
      println("Error : " + currentError)

       */



   //   if (currentError <= tol)
     //   break

       // Update indices
       for ( i <- 1 to ntables+1 ){
         // time for reset
         if ( num_iter % math.pow(npart, ntables + 2 - i) == 0)
           indices(i-1) = 1

         else if ( num_iter % math.pow(npart, ntables + 1 - i) == 0)
           indices(i-1) = indices(i-1) + 1
       }

      num_iter += 1
    }

    val endTime = System.nanoTime

    val elapsedMs = (endTime - startTime) / 1e6d

    println("Time taken : " + elapsedMs + "ms")
  }

 */



  def addColumnIndex(df: DataFrame, spark: SparkSession): DataFrame = {
    return spark.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map {
        case (row, index) => Row.fromSeq(row.toSeq :+ index)
      },
      // Create schema for index column
      StructType(df.schema.fields :+ StructField("index", LongType, false)))
  }


  def getTables(spark: SparkSession, query: String): Seq[String] = {
    val logicalPlan = spark.sessionState.sqlParser.parsePlan(query)
    import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
    logicalPlan.collect { case r: UnresolvedRelation => r.tableName}
  }


  /*
  Calculate sampling schemes for each table - unused for now

  def pick_samplers(join_inputs: Array[(String, String, String)]): scala.collection.mutable.Map[String, (String, String)] = {
    /*
   	 * Assign a sampling strategy to each table in the query.
   	 * There are 3 samplers at the moment:
   	 * 1. univ - universe
   	 * 2. unif - uniform
   	 * 3. strat - stratified
     */
    val num_tables = join_inputs.size + 1
    // (table_name, sampler_type, sample_key)
    var samplers: scala.collection.mutable.LinkedHashMap[String, (String, String)] = scala.collection.mutable.LinkedHashMap()
    // If only 2 tables, use universe sampling on both
    if (num_tables == 2) {
      samplers += (join_inputs(0)._1 -> ("univ", join_inputs(0)._3))
      samplers += (join_inputs(0)._2 -> ("univ", join_inputs(0)._3))
    } // If 3 tables, use universe on first 2, and uniform on second
    else if (num_tables == 3) {
      samplers += (join_inputs(0)._1 -> ("univ", join_inputs(0)._3))
      samplers += (join_inputs(0)._2 -> ("univ", join_inputs(0)._3))
      samplers += (join_inputs(1)._2 -> ("unif", join_inputs(1)._3))
    } // If 4 or more tables, look if a join sequence contains a subsequence of the same join keys
    else {
      var counts = scala.collection.mutable.Buffer[(String, Integer, Integer)]()
      var prevKey = ""
      var index = -1
      var currPos = 0

      // Calculate subsequence lengths
      for (ji <- join_inputs) {
        val currKey = ji._3

        // If next join key is the same, update its count
        if (currKey == prevKey) {
          //counts(index) = (counts(index)._1, counts(index)._2 + 1, counts(index)._3)
          counts(index) = counts(index).copy(_2 = counts(index)._2 + 1)
          currPos = currPos + 1

        } // Otherwise, add new join key and initialise its count
        else {
          index = index + 1
          counts.append((ji._3, 1, currPos))
          currPos = currPos + 1

        }
        prevKey = currKey
      }

      // Assign samplers
      breakable {
        for (tuple <- counts) {
          val max_elem = counts.maxBy(_._2)
          val i_max_elem = counts.indexOf(max_elem)
          counts(i_max_elem) = (counts(i_max_elem)._1, -1, counts(i_max_elem)._3)
          // Max element key
          val max_elem_key = max_elem._1
          // Key count
          val subseq_key_count = max_elem._2
          // Where we are in the join chain
          val pos = max_elem._3
          var sampler = ""

          if (subseq_key_count >= 2)
            sampler = "univ"
          else
            sampler = "unif"

          // Store (table, sampler, join_key)
          for (i <- 0 to subseq_key_count) {
            if (((pos + i) == join_inputs.size - 1 || i == subseq_key_count - 1) && !samplers.contains(join_inputs(pos + i)._1)) {
              samplers += (join_inputs(pos + i)._2 -> (sampler, max_elem_key))
            }

            if (!samplers.contains(join_inputs(pos + i)._1))
              samplers += (join_inputs(pos + i)._1 -> (sampler, max_elem_key))
          }

          // update neighbors
          if (i_max_elem == 0) {
            if (counts(i_max_elem + 1)._2 != -1)
              counts(i_max_elem + 1) = counts(i_max_elem + 1).copy(_2 = math.max(counts(i_max_elem + 1)._2 - 1, 0))
          } else if (i_max_elem == counts.size - 1) {
            if (counts(i_max_elem - 1)._2 != -1)
              counts(i_max_elem - 1) = counts(i_max_elem - 1).copy(_2 = math.max(counts(i_max_elem - 1)._2 - 1, 0))
          } else {
            if (counts(i_max_elem - 1)._2 != -1)
              counts(i_max_elem - 1) = counts(i_max_elem - 1).copy(_2 = math.max(counts(i_max_elem - 1)._2 - 1, 0))

            if (counts(i_max_elem + 1)._2 != -1)
              counts(i_max_elem + 1) = counts(i_max_elem + 1).copy(_2 = math.max(counts(i_max_elem + 1)._2 - 1, 0))
          }

        }
      }
    }

    return samplers
  }


  def compute_samples(spark: SparkSession) {
    val p = 0.1
    val tables = Array("lineitem", "orders", "customer")
    var db_schema = new ListBuffer[(String, List[String])]()
    var sampling_scheme: scala.collection.mutable.LinkedHashMap[String, List[String]] = scala.collection.mutable.LinkedHashMap()

    // Gather columns of each table
    for (table <- tables) {
      val cols = spark.sql("describe table " + table).select("col_name").collect().map(_(0)).toList
        .map(x => x.toString().substring(2, x.toString().length()))

      db_schema.append((table, cols))
    }

    for (i <- db_schema.indices) {
      for (j <- i until db_schema.length) {
        if (db_schema(i) != db_schema(j)) {
          val isect = List(db_schema(i)._2, db_schema(j)._2).reduce((a, b) => a intersect b).filter(_ != "comment")

          if (isect.nonEmpty) {

            sampling_scheme += sampling_scheme.get(db_schema(i)._1)
              .map(x => db_schema(i)._1 -> List.concat(x, isect)).getOrElse(db_schema(i)._1 -> isect)
            sampling_scheme += sampling_scheme.get(db_schema(j)._1)
              .map(x => db_schema(j)._1 -> List.concat(x, isect)).getOrElse(db_schema(j)._1 -> isect)
          }
        }
      }
    }
  }


  def load_tables(spark: SparkSession, query: String): Unit ={

    // Load the data as temp views
    val tables = getTables(spark, query)

    for (table <- tables){
      var tableDF = spark.read.parquet("partitioned/" + table)
      //tableDF = Eval.assign_subsamples(spark, tableDF, table, tableDF.count(), b)
      tableDF.createOrReplaceTempView(table)
    }

  }


   */
}