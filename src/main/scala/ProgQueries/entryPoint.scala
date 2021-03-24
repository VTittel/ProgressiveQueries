package ProgQueries
import FileHandlers.TableDefs
import org.apache.spark.sql.functions._

import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}
import scala.util.control.Breaks._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.{BufferedWriter, File, FileWriter}
import scala.language.postfixOps
import Query.{BenchQueries, QueryParser}

import scala.collection.mutable
import scala.reflect.io.Directory


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
      "univSampleFrac" -> "5",
      "dataDir" -> "partitioned_with_sid_sf10/",
      "warehouseDir" -> "sample_warehouse",
      "alpha" -> "0.05")

    val query = BenchQueries.QuerySix
    val qp = new QueryParser

    val joinInputs = qp.parseQueryJoin(spark, query)
    val spGen = new SamplingPlanGenerator
    val sampleChoices = spGen.pickSamplers(joinInputs)

    val numTables = getTables(spark, query).length

    if (numTables == 1)
      runSingleTableQuery(spark, query, params, Map[String, Any](), "0")
    else
      runMultiTableQuery(spark, query, params, Map[String, Any](), "0", sampleChoices)


    /*

    val query1 = """select (l_extendedprice*(1-l_discount)) as sum_revenue, o_shippriority, o_orderstatus, o_sid , c_sid , l_sid
                   |from customer join order on customer.c_custkey = order.o_custkey
                   |join lineitem on lineitem.l_orderkey = order.o_orderkey
                   |where o_orderdate < '1995-03-15'""".stripMargin

    val order1 = spark.read.parquet("partitioned_with_sid_sf10/order.parquet/unif_sample_group=1")
    val order2 = spark.read.parquet("partitioned_with_sid_sf10/order.parquet/unif_sample_group=2")
    val order3 = spark.read.parquet("partitioned_with_sid_sf10/order.parquet/unif_sample_group=3")
    val order4 = spark.read.parquet("partitioned_with_sid_sf10/order.parquet/unif_sample_group=4")

    val cust1 = spark.read.parquet("partitioned_with_sid_sf10/customer.parquet/unif_sample_group=1")
    val cust2 = spark.read.parquet("partitioned_with_sid_sf10/customer.parquet/unif_sample_group=2")
    val cust3 = spark.read.parquet("partitioned_with_sid_sf10/customer.parquet/unif_sample_group=3")
    val cust4 = spark.read.parquet("partitioned_with_sid_sf10/customer.parquet/unif_sample_group=4")

    val line1 = spark.read.parquet("partitioned_with_sid_sf10/lineitem.parquet/unif_sample_group=1")
    val line2 = spark.read.parquet("partitioned_with_sid_sf10/lineitem.parquet/unif_sample_group=2")
    val line3 = spark.read.parquet("partitioned_with_sid_sf10/lineitem.parquet/unif_sample_group=3")
    val line4 = spark.read.parquet("partitioned_with_sid_sf10/lineitem.parquet/unif_sample_group=4")

    val order = order1.union(order2).union(order3).union(order4)
    val cust = cust1.union(cust2).union(cust3).union(cust4)
    val line = line1.union(line2).union(line3).union(line4)

    order.createOrReplaceTempView("order")
    cust.createOrReplaceTempView("customer")
    line.createOrReplaceTempView("lineitem")

    spark.sql(query).show()
    */

    //spark.read.csv("q6exactsf10.csv").show()
  //  runExactQuery(spark, query)


    println("Program finished")
   // System.in.read();
   // spark.stop();
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


    val qp = new QueryParser()
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
    val aggregates: ArrayBuffer[(String, String, String)] = qp.parseQueryAggregate(logicalPlan, hasGroupBy)

    val execTimes = scala.collection.mutable.ListBuffer.empty[Double]
    val errors = ListBuffer.empty[ListBuffer[(String, String, String, Double)]]

    while (i < maxIterations) {
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

      val groupings: Seq[String] = qp.parseQueryGrouping(logicalPlan)
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
      // scale factor
      // TODO: fix sf
      val sf = 100 - i
      val res = Eval.evaluatePartialResult(result, params, aggregates, sf)
      val errorsForIter = ListBuffer.empty[(String, String, String, Double)]

      var resultMap = scala.collection.mutable.Map[String,Any]()

      for (evalResult <- res) {
        val agg = evalResult("agg")
        val est = evalResult("est")
        val groupError = evalResult("error").toDouble
        val group = evalResult("group")

        errorsForIter.append((agg, group, est, groupError))
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
    // if (currentErrors.count(_ < errorTol) == currentErrors.length)
     //   break

      currentErrors.clear()
      errors.append(errorsForIter)

      i += 1

      val endTime = System.nanoTime
      val elapsedMs = Math.round((endTime - startTime) / 1e6d)
      execTimes += elapsedMs

      println("*****Iteration " + i + " complete*****")
    }

    println(execTimes.mkString(","))
    writeErrorsToFile(errors)

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
                         queryStorage: Map[String,Any], qID: String,
                         sampleChoices: Map[String, (String, String)]): Unit = {


    // Sample size, expressed in % of total table size. Only discrete values between 0-100 are possible for now
    val maxIterations: Integer = params("samplePercent").toInt
    val b = params("b").toInt
    val dataDir = params("dataDir")
    val warehouseDir = params("warehouseDir")
    val univSampleFrac = params("univSampleFrac").toDouble
    // Error tolerance, i.e. when to stop
    val errorTol = params("errorTol").toDouble
    // Keep track of error at each iteration
    val currentErrors = ArrayBuffer[Double]()

    val qp = new QueryParser()
    val Eval = new Evaluation()
    val spGen = new SamplingPlanGenerator
    //Do online universe sampling
    spGen.makeUnivSamples(spark, sampleChoices, 0, 5, dataDir )

    // All results computed so far
    var runningResult = spark.emptyDataFrame
    val tableNames = sampleChoices.keys.toList
    var filesPerTable = Map.empty[String, List[File]]

    // Query rewrite depends if the query has a group by clause
    val hasGroupBy:Boolean = query.contains("group by")
    val logicalPlan = spark.sessionState.sqlParser.parsePlan(query)
    val aggregates: ArrayBuffer[(String, String, String)] = qp.parseQueryAggregate(logicalPlan, hasGroupBy)

    // TODO: Handle region/nation parquets properly
    for((k,v) <- sampleChoices){
      if (k == "region" || k == "nation") { // Dont sample small tables
        filesPerTable += (k -> List(new File(dataDir + k + ".parquet")))
      } else if (v._1 == "unif") { // Use existing uniform samples
        val files: List[File] = sortWithSampleNr(getListOfFiles(new File(dataDir + k + ".parquet"), excludedFiles))
        filesPerTable += (k -> files)
      } else if (v._1 == "univ"){ // Collect universe samples
        val warehousedSamples = getListOfFiles(new File(warehouseDir), excludedFiles).map(_.toString)
          .filter(_.contains(k + "_" + v._2)).head

        val files: List[File] = sortWithSampleNr(getListOfFiles(new File(warehousedSamples), excludedFiles))
        filesPerTable += (k -> files)
      }
    }


    var i = 0
    // TODO: Turn this into an sql function
    val sidUDF = udf(h _)

    // Cache samples for each table
    var sampleCache: mutable.Map[String, (ListBuffer[String], DataFrame)] = mutable.Map()
    val newSamples: mutable.Map[String, DataFrame] = mutable.Map()

    val execTimes = scala.collection.mutable.ListBuffer.empty[Double]
    val errors = ListBuffer.empty[ListBuffer[(String, String, String, Double)]]
    val estimates = ListBuffer.empty[ListBuffer[(String, Double)]]

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
      queryWithoutAgg = queryWithoutAgg.substring(0, queryWithoutAgg.indexOf("from")) + ", " + table.take(1) + "_" +
        "sid " + queryWithoutAgg.substring(queryWithoutAgg.indexOf("from"), queryWithoutAgg.length)

    val joinGraph = new JoinGraph()

    while (i < 5) {
      val startTime = System.nanoTime

      // For each table, update its entry in newTableSamples with the new sample
      for (tableName <- tableNames){
        if (tableName == "region" || tableName == "nation") {
          val sample = spark.read.parquet(filesPerTable(tableName).head.toString)
          newSamples.get(tableName) match {
            case None => newSamples.put(tableName, sample)
            case Some(v) => newSamples(tableName) = sample
          }
        }
        else {
          val sample = spark.read.parquet(filesPerTable(tableName)(i).toString)
          newSamples.get(tableName) match {
            case None => newSamples.put(tableName, sample)
            case Some(v) => newSamples(tableName) = sample
          }
        }
        // update graph vertices
        val index = tableNames.indexOf(tableName)
        var vertex = Option.empty[joinGraph.Node].orNull

        if (index == 0)
          vertex = new joinGraph.Node(filesPerTable(tableName)(i).toString, "start", "unif", tableName, i)
        else if (index == tableNames.length-1)
          vertex = new joinGraph.Node(filesPerTable(tableName)(i).toString, "end", "unif", tableName, i)
        else
          vertex = new joinGraph.Node(filesPerTable(tableName)(i).toString, "interm", "unif", tableName, i)

        joinGraph.addVertex(vertex)
      }

      for (i <- 0 to tableNames.length-2)
        joinGraph.addEdges(tableNames(i), tableNames(i+1))


      val paths: ListBuffer[ListBuffer[Set[joinGraph.Node]]] = joinGraph.calculatePaths()
      var partial = spark.emptyDataFrame

      for (path <- paths){

        for (nodeSet <- path){
          val nodeSetSorted = nodeSet.toList.sortWith(_.getName() < _.getName())
          val nodeSetSortedNames = nodeSetSorted.map(x => x.getName())

          if (i == 0){
            for (e <- nodeSetSorted) {
              newSamples(e.tName).createOrReplaceTempView(e.tName)
            }
          } else{
            /*
            Three cases are possible:
            1. The node set consists of only a new node
              => sampleCache(tName).nodeNames.size >= nodeSet.size && sampleCache(tName).nodeNames.intersection(nodeSet) == {}
              = create view of that node only
            2. The node set consists of only old nodes already in the cache
              => sampleCache(tName).nodeNames == nodeSet
              = create a view of the stored nodes
            3. The nodeset consists of old and new nodes.
              => sampleCache(tName).nodeNames.subsetOf(nodeSet) && sampleCache(tName).nodeNames.size < nodeSet.size
              = create a view of the stored nodes union the new node
            */

            val tName = nodeSet.head.tName
            val cachedSet = sampleCache(tName)._1.toSet

            // Case 1
            if (cachedSet.size >= nodeSet.size && cachedSet.intersect(nodeSetSortedNames.toSet).isEmpty) {
              newSamples(tName).createOrReplaceTempView(tName)
            } // Case 2
            else if (cachedSet.equals(nodeSetSortedNames.toSet)) {
              sampleCache(tName)._2.createOrReplaceTempView(tName)
            } // Case 3
            else if (cachedSet.subsetOf(nodeSetSortedNames.toSet) && cachedSet.size < nodeSetSortedNames.toSet.size) {
              sampleCache(tName)._2.union(newSamples(tName)).createOrReplaceTempView(tName)
            }
          }
        }

        // Join result without aggregation
        val pathDF = spark.sql(queryWithoutAgg)

        if (paths.indexOf(path) == 0)
          partial = pathDF
        else
          partial = partial.union(pathDF)
      }

      // Update sampleCache
      for (tableName <- tableNames){
        sampleCache.get(tableName) match {
          case None => sampleCache.put(tableName, (ListBuffer(filesPerTable(tableName)(i).toString), newSamples(tableName)))
          case Some(v) => sampleCache(tableName) = (sampleCache(tableName)._1 ++
            ListBuffer(filesPerTable(tableName)(i).toString),
            sampleCache(tableName)._2.union(newSamples(tableName)))
        }
      }

      /*
      Step 1: Join all tuples, without computing aggregate, so that we can compute their combined sid
       */
      // Assign sid's to result tuples
      val sidColumns = partial.schema.fieldNames.filter( col => col.contains("_sid"))
      var resultWithSid = partial.withColumn("sid", sidUDF(lit(b), struct(partial.columns map col: _*),
        array(sidColumns.map(lit(_)): _*)))
      // Result with newly assigned SIDs
     //resultWithSid = resultWithSid.where("sid != 0")
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
      val groupings: Seq[String] = qp.parseQueryGrouping(logicalPlan)
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
      val sf = calculateSF(sampleChoices, i, univSampleFrac)

      val res = Eval.evaluatePartialResult(resultAggregatedWithSid, params, aggregates, sf)
      val errorsForIter = ListBuffer.empty[(String, String, String, Double)]

      var resultMap = scala.collection.mutable.Map[String,Any]()

      val estForIter = ListBuffer.empty[(String, Double)]

      for (evalResult <- res) {
        val agg = evalResult("agg")
        val est = evalResult("est")
        val groupError = evalResult("error").toDouble
        val group = evalResult("group")
        errorsForIter.append((agg, group, est, groupError))
        currentErrors.append(groupError)

        var resultPerGroup = scala.collection.mutable.Map[String,String]()
        resultPerGroup += ("Estimate" -> evalResult("est"))
        resultPerGroup += ("CI_low" -> evalResult("ci_low"))
        resultPerGroup += ("CI_high" -> evalResult("ci_high"))
        resultPerGroup += ("Error" -> groupError.toString)

        resultMap += ((evalResult("agg"), evalResult("group")).toString() ->
          scala.util.parsing.json.JSONObject(resultPerGroup.toMap))

        estForIter.append((group, est.toDouble))

        if (group == "O,0" || group == "0,O") {
          val err = math.abs(est.toDouble - 129855599.1545) / 129855599.1545
          println("Aggregate: " + agg + " || " + "Group : " + group + " || " + "Error : " + err)
        } else if (group == "F,0" || group == "0,F"){
          val err = math.abs(est.toDouble - 1049506760932.4486) / 1049506760932.4486
          println("Aggregate: " + agg + " || " + "Group : " + group + " || " + "Error : " + err)
        } else if (group == "P,0" || group == "0,P"){
          val err = math.abs(est.toDouble - 9459047234.3098) / 9459047234.3098
          println("Aggregate: " + agg + " || " + "Group : " + group + " || " + "Error : " + err)
        }
      }

      queryStorage += (qID -> scala.util.parsing.json.JSONObject(resultMap.toMap))

     // if (currentErrors.count(_ < errorTol) == currentErrors.length)
      //  break

      currentErrors.clear()
      estForIter.clear()
      errors.append(errorsForIter)
      estimates.append(estForIter)

      println("*****Iteration " + i + " complete*****")

      val endTime = System.nanoTime
      val elapsedMs = Math.round((endTime - startTime) / 1e6d)
      execTimes += elapsedMs

      i += 1

    }

    //println(estimates)
    println(execTimes.mkString(","))
    //writeEstToFile(estimates)
   // writeErrorsToFile(errors)
    // Delete sample warehouse after query is done.
    // TODO: Reuse warehoused samples, and create additional samples as needed.
    val directory = new Directory(new File(warehouseDir))
    directory.deleteRecursively()
  }

  // TODO: Fix scaling factor to account for universe sampling
  def calculateSF(sampleChoices: Map[String, (String, String)], i: Integer, univFrac: Double): Double ={

    var samplingFrac = 1.0
    for ((k,v) <- sampleChoices){
      if (v._1 == "unif"){
        samplingFrac *= (i.toDouble+1.0) / 100
      } else if (v._1 == "univ"){
        samplingFrac *= ((i.toDouble +1.0)*univFrac) / 100
      }
    }
      //math.pow((i.toDouble+1.0) / 100, tableNames.size.toDouble)
    //    val samplingFrac = ((i.toDouble+1.0) * 10.0) / 100 * 0.01
    val sf = 1.0 / samplingFrac
    return sf
  }


  def writeEstToFile(errors : ListBuffer[ListBuffer[(String, Double)]]): Unit ={
    val file = new File("q3unifEst.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    for (error <- errors) {
      bw.write(error.mkString(","))
      bw.write("\n")
    }
    bw.close()
  }


  def writeErrorsToFile(errors : ListBuffer[ListBuffer[(String, String, String, Double)]]): Unit ={
    val file = new File("spark-warehouse/errors.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    for (error <- errors) {
      bw.write(error.mkString(" "))
      bw.write("\n")
    }
    bw.close()
  }

  /* Extract tables from query */
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



  def runExactQuery(spark: SparkSession, query: String): Unit ={
    TableDefs.load_tpch_tables(spark, "partitioned_with_sid_sf10/")
    //spark.sql(query).coalesce(1).write.csv("spark-warehouse/query" + qID + ".csv")

    spark.sql(query).coalesce(1).write.option("header", "true").csv("q6exactsf10" + ".csv")

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


}