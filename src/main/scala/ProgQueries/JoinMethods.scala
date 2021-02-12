package ProgQueries

class JoinMethods {

  /*
  // TODO: add group by support
  def runJoinQuery(spark: SparkSession, query: String, joinInputs: Array[(String, String, String)],
                   agg: (String, String), params: Map[String, String]){

    // Number of tables
    val tableCount = joinInputs.length
    val partitionCount: Double = params("sampleSize").toDouble
    val b = params("b").toInt
    val errorTol = params("errorTol").toDouble
    val topDir = params("dataDir")

    // All results computed so far
    var runningResult = spark.emptyDataFrame

    // Keep track of error at each iteration
    var currentError = errorTol + 1
    val totalIterations = math.pow(partitionCount, tableCount + 1)
    // Keep track of join partitions
    val indices: Array[Int] = Array.fill(tableCount + 1)(1)

    val Eval = new Evaluation()

    val tableNames: Seq[String] = getTables(spark, query)
    var filesPerTable = mutable.Map.empty[String, List[File]]

    for (tableName <- tableNames){
      val files: List[File] = sortWithSampleNr(getListOfFiles(new File(topDir + tableName + ".parquet"), excludedFiles))
      filesPerTable += (tableName -> files)
    }


    var i = 1

    val startTime = System.nanoTime

    while (i <= 1) {
      // Modify query and do the join for current batch
      // Bucket index, which bucket we should use
      var j = 0
      var newQuery = query
      for (tableName <- tableNames){
        /*
        val fromIndex = newQuery indexOf "from"
        newQuery = newQuery.substring(0, fromIndex) + "," + table.take(1) + "_sid " +
          newQuery.substring(fromIndex, newQuery.length)
         */
        // Replace agg(col) by col - can only do aggregate after we calculate the new sid
        newQuery = newQuery.replace(agg._1 + "(" + agg._2 + ")", agg._2)
        newQuery = newQuery.replaceFirst(" " + tableName, " parquet.`" +
          filesPerTable(tableName)(indices(j)-1) + "`" + " as " + tableName)

        j += 1
      }

      // Join result without aggregation
      val partial = spark.sql(newQuery)

      // TODO: Turn this into an sql function
      val sidUDF = udf(h _)

      // Assign sid's to result tuples
      val sidColumns = partial.schema.fieldNames.filter( col => col.contains("_sid"))
      var resultWithSid = partial.withColumn("sid", sidUDF(lit(b), struct(partial.columns map col: _*),
        array(sidColumns.map(lit(_)): _*)))
      //  resultWithSid.cache()  // needed, otherwise filter doesnt work
      // Result with newly assigned SIDs
      resultWithSid = resultWithSid.where("sid != 0")


      // Add new samples to old ones
      if ( i == 1)
        runningResult = resultWithSid
      else
        runningResult = runningResult.union(resultWithSid)

      runningResult.createOrReplaceTempView("join_result")

      val aggQuery = "select " + agg._1 + "(" + agg._2 + ")" + " from join_result group by sid"
      // Result grouped by newly assigned SIDs
      val resultGroupedBySid = spark.sql(aggQuery)
      resultGroupedBySid.createOrReplaceTempView("join_result")

      // Evaluate new result
      val res = Eval.evaluatePartialResult(resultGroupedBySid, params, agg, i+1)
      currentError = res("error").toDouble

      println("Result :" + res("est"))
      println("CI : " + "[" + res("ci_low") + " , " + res("ci_high") + "]")
      println("Error : " + currentError)


      //   if (currentError <= tol)
      //   break

      // Update indices
      for ( k <- 1 to tableCount + 1 ){
        // time for reset
        if ( i % math.pow(partitionCount, tableCount + 2 - k) == 0)
          indices(k-1) = 1

        else if ( i % math.pow(partitionCount, tableCount + 1 - k) == 0)
          indices(k-1) = indices(k-1) + 1
      }

      i += 1
    }

    val endTime = System.nanoTime
    val elapsedMs = (endTime - startTime) / 1e6d
    println("Time taken : " + elapsedMs + "ms")


    //System.in.read();
    //spark.stop();
  }

def runJoinQuery(spark: SparkSession, query: String, joinKey: String,
                   agg: (String, String), params: Map[String, String]){

    // Number of tables
    val partitionCount: Double = params("sampleSize").toDouble
    val b = params("b").toInt
    val errorTol = params("errorTol").toDouble
    val topDir = params("dataDir")

    // Keep track of error at each iteration
    var currentError = errorTol + 1

    val Eval = new Evaluation()

    val tableNames: Seq[String] = getTables(spark, query)
    var filesPerTable = mutable.Map.empty[String, List[File]]

    for (tableName <- tableNames){
      val files: List[File] = sortWithSampleNr(getListOfFiles(new File(topDir + tableName + ".parquet"), excludedFiles))
      filesPerTable += (tableName -> files)
    }

    var i = 1

    val execTimes = scala.collection.mutable.ListBuffer.empty[Double]
    val errors = scala.collection.mutable.ListBuffer.empty[Double]

    while (i <= 5) {
      val startTime = System.nanoTime
      // Modify query and do the join for current batch
      // Bucket index, which bucket we should use
      var j = 0
      var queryWithoutAgg = query.replace(agg._1 + "(" + agg._2 + ")", agg._2)

      for (tableName <- tableNames) {
        val QR = new QueryRewriter()
        var cols:Array[String] = QR.extractColumnsFromQuery(queryWithoutAgg)
        cols = cols :+ tableName.take(1) + "_" + joinKey
        queryWithoutAgg = QR.rewriteToUnion(queryWithoutAgg, filesPerTable(tableName).slice(0, i), tableName, cols)

        j += 1
      }

      // Join result without aggregation
      val partial = spark.sql(queryWithoutAgg)

      // TODO: Turn this into an sql function
      val sidUDF = udf(h _)

      // Assign sid's to result tuples
      val sidColumns = partial.schema.fieldNames.filter( col => col.contains("_sid"))
      var resultWithSid = partial.withColumn("sid", sidUDF(lit(b), struct(partial.columns map col: _*),
        array(sidColumns.map(lit(_)): _*)))
      // Result with newly assigned SIDs
      resultWithSid = resultWithSid.where("sid != 0")


      resultWithSid.createOrReplaceTempView("join_result")

      val aggQuery = "select " + agg._1 + "(" + agg._2 + ")" + " from join_result group by sid"
      // Result grouped by newly assigned SIDs
      val resultGroupedBySid = spark.sql(aggQuery)
      resultGroupedBySid.createOrReplaceTempView("join_result")

      // Evaluate new result
      val res = Eval.evaluatePartialResult(resultGroupedBySid, params, agg, i+1)
      currentError = res("error").toDouble
      errors += currentError

      println(i)
      println("Result :" + res("est"))
      println("CI : " + "[" + res("ci_low") + " , " + res("ci_high") + "]")
      println("Error : " + currentError)


       //   if (currentError <= tol)
         //   break

      i += 1

      val endTime = System.nanoTime
      val elapsedMs = Math.round((endTime - startTime) / 1e6d)
      execTimes += elapsedMs

    //  println("Time taken : " + elapsedMs + "ms")
    }

    println(execTimes.mkString(","))
    println(errors.mkString(","))

    //System.in.read();
    //spark.stop();

   */


}
