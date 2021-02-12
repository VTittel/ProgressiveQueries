package ProgQueries
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan, Project}
import org.apache.spark.sql.functions._

import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}
import scala.util.control.Breaks._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.mutable
import java.io.{BufferedWriter, File, FileWriter}


object entryPoint {
  val excludedFiles = List("crc", "_SUCCESS")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark SQL basic example")
      //.config("spark.master", "spark://mcs-computeA002:7077")
      .config("spark.master", "local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val params = Map("errorTol" -> "2.0",
      "samplePercent" -> "10",
      "b" -> "100",
      "dataDir" -> "partitioned_with_sid_sf10/",
      "alpha" -> "0.05")


    val query = BenchQueries.QueryTwo
  //  TableDefs.load_tpch_tables(spark, "partitioned_with_sid_sf10/")
   // spark.sql(query).show()

    val numTables = getTables(spark, query).length

    if (numTables == 1)
      runSingleTableQuery(spark, query, params, Map[String, Any](), "0")
    else
      runMultiTableQuery(spark, query, params, Map[String, Any](), "0")


    println("Program finished")
   // System.in.read();
   // spark.stop();
  }


  /*
  Node type:
  r: Aggregate => r.aggregateExpressions
  r: Project => r.projectList works for when there is no group by
   */
  def parseQueryAggregate(logicalPlan: LogicalPlan, hasGroupBy: Boolean): ArrayBuffer[(String, String, String)] ={

    val result = ArrayBuffer[(String, String, String)]()
    var ast = Seq[Seq[NamedExpression]]()

    if (hasGroupBy)
      ast = logicalPlan.collect { case r: Aggregate => r.aggregateExpressions}
    else
      ast = logicalPlan.collect { case r: Project => r.projectList}

    for (aggNode <- ast.head) {
      if (aggNode.children.nonEmpty) {
        val aggString = aggNode.children.head.toString().replaceAll("'|\\s", "")
        val braceIndex = aggString indexOf "("
        val aggFunc = aggString.substring(0, braceIndex)
        var aggCol = aggString.substring(braceIndex, aggString.length).replaceAll("\\s", "")
        aggCol =  aggCol.substring(aggCol.indexOf("(")+1, aggCol.lastIndexOf(")"))
        val aggNodeStr = aggNode.toString()
        val alias = aggNodeStr.substring((aggNodeStr indexOf "AS") + 3, aggNodeStr indexOf "#")
        result.append((aggFunc, aggCol, alias))
      }
    }
    return result
  }


  /*
  Node type:
  r: Aggregate => r.groupingExpressions
 */

  def parseQueryGrouping(logicalPlan: LogicalPlan): Seq[String] ={

    val ast = logicalPlan.collect { case r: Aggregate => r.groupingExpressions}

    if (ast.isEmpty)
      return Seq()

    val resultAst = ast.head.map(col => col.toString().replaceAll("'|\\s", ""))
    return resultAst
  }


  def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isDirectory).toList.filter { file =>
      !extensions.exists(file.getName.endsWith(_  ))
    }
  }


  /* Sort files containing samples */
  def sortWithSampleNr(files: List[File]): List[File] ={
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

  def runSingleTableQuery(spark: SparkSession, query: String, params: Map[String, String],
                          queryStorage: Map[String,Any], qID: String): Unit ={

    // Sample size, expressed in % of total table size. Only discrete values between 0-100 are possible for now
    val maxIterations: Integer = params("samplePercent").toInt
    val topDir = params("dataDir")

    // Error tolerance, i.e. when to stop
    val errorTol = params("errorTol").toDouble

    // Keep track of error at each iteration
    val currentErrors = ArrayBuffer[Double]()
    var i: Integer = 0

    var result = spark.emptyDataFrame

    val Eval = new Evaluation()

    // Get names of all subdirectories in tableName.parquet dir
    val tableName = getTables(spark, query).head
    val files = sortWithSampleNr(getListOfFiles(new File(topDir + tableName + ".parquet"), excludedFiles))
    // Query rewrite depends if the query has a group by clause
    val hasGroupBy:Boolean = query.contains("group by")
    val logicalPlan = spark.sessionState.sqlParser.parsePlan(query)
    val aggregates: ArrayBuffer[(String, String, String)] = parseQueryAggregate(logicalPlan, hasGroupBy)

    val execTimes = scala.collection.mutable.ListBuffer.empty[Double]
    val errors = ListBuffer.empty[ListBuffer[(String, String, Double)]]

    while (i <= maxIterations) {
      val startTime = System.nanoTime
      /*
      Rewrite the query to directly load the parquet file from the query, and add group by sid.
      */
      var newQuery = ""

      if (hasGroupBy)
        newQuery = query.replace(" " + tableName, " parquet.`" + files(i) + "`") + "," +
          tableName.take(1) + "_sid"
      else
        newQuery = query.replace(" " + tableName, " parquet.`" + files(i) + "`") +
        " group by " + tableName.take(1) + "_sid"


      val partial = spark.sql(newQuery)

      // Accumulate samples
      if ( i == 0)
        result = partial
      else
        result = result.union(partial)


      result.createOrReplaceTempView("result")

      val groupings: Seq[String] = parseQueryGrouping(logicalPlan)
      val cols: Seq[String] = result.columns.toSeq
      var aggQuery = "select "

      // Hack to check if a column is an aggregate : check if it contains a bracket (
      for (col <- cols){
        aggQuery = aggQuery + (if(col.contains("(")) col.substring(0, col.indexOf("(")) + "(`" +  col + "`) " + " as `" +
          col + "`" else col) + ","
      }
      // Delete the last comma
      aggQuery = aggQuery.dropRight(1)

      if (hasGroupBy)
        aggQuery = aggQuery + " from result group by " + groupings.mkString(",")

      else
        aggQuery = aggQuery + " from result"

      // Evaluate new result

      val res = Eval.evaluatePartialResult(result, params, aggregates, i+1)
      val errorsForIter = ListBuffer.empty[(String, String, Double)]

      var resultMap = scala.collection.mutable.Map[String,Any]()

      for (evalResult <- res) {
        val agg = evalResult("agg")
        val groupError = evalResult("error").toDouble
        val group = evalResult("group")
        errorsForIter.append((agg, group, groupError))
        currentErrors.append(groupError)

        var resultPerGroup = scala.collection.mutable.Map[String,String]()
        resultPerGroup += ("Estimate" -> evalResult("est"))
        resultPerGroup += ("CI_low" -> evalResult("ci_low"))
        resultPerGroup += ("CI_high" -> evalResult("ci_high"))
        resultPerGroup += ("Error" -> groupError.toString)

        resultMap += ((evalResult("agg"), evalResult("group")).toString() ->
          scala.util.parsing.json.JSONObject(resultPerGroup.toMap))
      }

      queryStorage += (qID -> scala.util.parsing.json.JSONObject(resultMap.toMap))

      // Break if accuracy is achieved
      // TODO: Fix for groups that have too little tuples, and thus 0 error
      if (currentErrors.count(_ < errorTol) == currentErrors.length)
        break

      currentErrors.clear()
      errors.append(errorsForIter)

      i += 1

      val endTime = System.nanoTime
      val elapsedMs = Math.round((endTime - startTime) / 1e6d)
      execTimes += elapsedMs

      println("*****Iteration " + i + " complete*****")
    }

    //println(execTimes.mkString(","))
    //writeErrorsToFile(errors)

  }


  /*
  Method to execute a query on multiple tables progressively.
  @param spark, the current spark Session
  @param query, String of sql query
  @param joinInputs, array of triples where each triple is the form (table1, table2, joinAttribute)
  @param params: query and evaluation parameters
  @param agg: type of aggregate function and column its applied on
  @return nothing
   */

  def runMultiTableQuery(spark: SparkSession, query: String, params: Map[String, String],
                         queryStorage: Map[String,Any], qID: String): Unit = {

    // Sample size, expressed in % of total table size. Only discrete values between 0-100 are possible for now
    val maxIterations: Integer = params("samplePercent").toInt
    val b = params("b").toInt
    val topDir = params("dataDir")
    // Error tolerance, i.e. when to stop
    val errorTol = params("errorTol").toDouble
    // Keep track of error at each iteration
    val currentErrors = ArrayBuffer[Double]()

    // All results computed so far
    var runningResult = spark.emptyDataFrame

    val Eval = new Evaluation()

    val tableNames: Seq[String] = getTables(spark, query)
    var filesPerTable = mutable.Map.empty[String, List[File]]
    // Query rewrite depends if the query has a group by clause
    val hasGroupBy:Boolean = query.contains("group by")
    val logicalPlan = spark.sessionState.sqlParser.parsePlan(query)
    val aggregates: ArrayBuffer[(String, String, String)] = parseQueryAggregate(logicalPlan, hasGroupBy)

    // TODO: Handle region/nation parquets properly
    for (tableName <- tableNames){
      if (tableName == "region" || tableName == "nation")
        filesPerTable += (tableName -> List(new File(topDir + tableName + ".parquet")))
      else {
        val files: List[File] = sortWithSampleNr(getListOfFiles(new File(topDir + tableName + ".parquet"), excludedFiles))
        filesPerTable += (tableName -> files)
      }

    }

    var i = 0
    // TODO: Turn this into an sql function
    val sidUDF = udf(h _)

    val runningTables = Array.ofDim[DataFrame](tableNames.length)

    val execTimes = scala.collection.mutable.ListBuffer.empty[Double]
    val errors = ListBuffer.empty[ListBuffer[(String, Double)]]

    // remove aggregates and group by

    var queryWithoutAgg = query
    for (agg <- aggregates) {
      queryWithoutAgg = queryWithoutAgg.replace(agg._1 + "(" + agg._2 + ")" + " as " + agg._3, agg._2 + " as " + agg._3)
    }

    // Remove group by, since we removed aggregates
    if (hasGroupBy)
      queryWithoutAgg = queryWithoutAgg.substring(0, queryWithoutAgg.indexOf("group"))


    // Add SID projections for each table in join
    for (table <- tableNames)
      queryWithoutAgg = queryWithoutAgg.substring(0, queryWithoutAgg.indexOf("from")) + ", " + table.take(1) + "_"+
        "sid " + queryWithoutAgg.substring(queryWithoutAgg.indexOf("from"), queryWithoutAgg.length)

    val newTableSamples = Array.ofDim[DataFrame](tableNames.length)

    while (i <= maxIterations) {
      val startTime = System.nanoTime

      // For each table, update its entry in newTableSamples with the new sample
      for (j <- tableNames.indices){
        val tableName = tableNames(j)
        if (tableName == "region" || tableName == "nation")
          newTableSamples.update(j, spark.read.parquet(filesPerTable(tableName).head.toString))
        else
          newTableSamples.update(j, spark.read.parquet(filesPerTable(tableName)(i).toString))
      }

      // Join result without aggregation
      var partial = spark.emptyDataFrame

      if (i == 0){
        for (j <- tableNames.indices) {
          newTableSamples(j).createOrReplaceTempView(tableNames(j))
          runningTables.update(j,newTableSamples(j))
        }

        partial = spark.sql(queryWithoutAgg)
      }
      else {
      //  var partialPerTable = Array.ofDim[DataFrame](tableNames.length)
        // Join 1
        for (j <- tableNames.indices){
          if (j % 2 == 0){
            newTableSamples(j).createOrReplaceTempView(tableNames(j))
          } else {
            runningTables.update(j, runningTables(j).union(newTableSamples(j)))
            runningTables(j).createOrReplaceTempView(tableNames(j))
          }
        }

        val partial1 = spark.sql(queryWithoutAgg)

        // Join 2
        for (j <- tableNames.indices){
          if (j % 2 == 0){
            runningTables(j).createOrReplaceTempView(tableNames(j))
            runningTables.update(j, runningTables(j).union(newTableSamples(j)))
          } else {
            newTableSamples(j).createOrReplaceTempView(tableNames(j))
          }
        }

        val partial2 = spark.sql(queryWithoutAgg)

        partial = partial1.union(partial2)
      }


      /*
      Step 1: Join all tuples, without computing aggregate, so that we can compute their combined sid
       */
      // Assign sid's to result tuples
      val sidColumns = partial.schema.fieldNames.filter( col => col.contains("_sid"))
      var resultWithSid = partial.withColumn("sid", sidUDF(lit(b), struct(partial.columns map col: _*),
        array(sidColumns.map(lit(_)): _*)))
      // Result with newly assigned SIDs
      resultWithSid = resultWithSid.where("sid != 0")
      for (table <- tableNames)
        resultWithSid = resultWithSid.drop(table.take(1) + "_sid")

      // Add new samples to old ones
      if ( i == 0)
        runningResult = resultWithSid
      else
        runningResult = runningResult.union(resultWithSid)

      runningResult.createOrReplaceTempView("result")

      /*
      Step 2: Compute the aggregates on the joined tuples, group by sid
       */
      val groupings: Seq[String] = parseQueryGrouping(logicalPlan)
      val cols: Seq[String] = resultWithSid.columns.toSeq
      var aggQueryWithSid = "select "


      // Hack to check if a column is an aggregate : check if it contains a bracket (
      for (col <- cols){
        aggQueryWithSid = aggQueryWithSid + (if(aggregates.map(_._3).contains(col)) aggregates.find(_._3 == col).get._1 + "(" +
          col + ")" + " as " + col else col) + ","
      }
      // Delete the last comma
      aggQueryWithSid = aggQueryWithSid.dropRight(1)

      if (hasGroupBy)
        aggQueryWithSid = aggQueryWithSid + " from result group by " + groupings.mkString(",") + ", sid"

      else
        aggQueryWithSid = aggQueryWithSid + " from result group by sid"


      var resultAggregatedWithSid = spark.sql(aggQueryWithSid)
      resultAggregatedWithSid = resultAggregatedWithSid.drop("sid")
      resultAggregatedWithSid.createOrReplaceTempView("result")

      // Evaluate new result
      // scale factor
      val sf = 100 / (i+1)
      val res = Eval.evaluatePartialResult(resultAggregatedWithSid, params, aggregates, sf)
      val errorsForIter = ListBuffer.empty[(String, Double)]

      var resultMap = scala.collection.mutable.Map[String,Any]()

      for (evalResult <- res) {
    //    println(evalResult)
        val groupError = evalResult("error").toDouble
        val group = evalResult("group")
        errorsForIter.append((group, groupError))
        currentErrors.append(groupError)

        var resultPerGroup = scala.collection.mutable.Map[String,String]()
        resultPerGroup += ("Estimate" -> evalResult("est"))
        resultPerGroup += ("CI_low" -> evalResult("ci_low"))
        resultPerGroup += ("CI_high" -> evalResult("ci_high"))
        resultPerGroup += ("Error" -> groupError.toString)

        resultMap += ((evalResult("agg"), evalResult("group")).toString() ->
          scala.util.parsing.json.JSONObject(resultPerGroup.toMap))
      }

      queryStorage += (qID -> scala.util.parsing.json.JSONObject(resultMap.toMap))

      if (currentErrors.count(_ < errorTol) == currentErrors.length)
        break

      currentErrors.clear()
      errors.append(errorsForIter)

      i += 1

      println("*****Iteration " + i + " complete*****")

      val endTime = System.nanoTime
      val elapsedMs = Math.round((endTime - startTime) / 1e6d)
      execTimes += elapsedMs
    }

    //println(execTimes.mkString(","))
    //writeErrorsToFile(errors)

  }


  def writeErrorsToFile(errors : ListBuffer[ListBuffer[(String, String, Double)]]): Unit ={
    val file = new File("spark-warehouse/errors.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    for (error <- errors) {
      bw.write(error.mkString(" "))
      bw.write("\n")
    }
    bw.close()
  }


  def getTables(spark: SparkSession, query: String): Seq[String] = {
    val logicalPlan = spark.sessionState.sqlParser.parsePlan(query)
    import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
    logicalPlan.collect { case r: UnresolvedRelation => r.tableName}
  }


  /*
  h(i,j,..) function to calculated the new sid from VerdictDB
  */
  def h( b: Integer, row: Row, sidFields: Seq[String]): String = {
    val joinSize = sidFields.size
    var h = 1.0

    for (i <- 0 until joinSize){
      val sid = row.getAs(sidFields(i)).toString.toDouble
      if (sid == 0)
        return "0"
      else
        h += Math.floor(sid / math.sqrt(b.doubleValue())) * math.pow(math.sqrt(b.doubleValue()), joinSize - 1 - i)
    }

    return h.toString
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