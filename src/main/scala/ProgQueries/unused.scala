

package ProgQueries

object unused {
 // Create variational table for transactions and customers
  
    /*
      
        val transactions = spark.read.format("csv")
                .option("header", "true").load("Transactions.csv");
    val customers = spark.read.format("csv")
                .option("header", "true").load("Customers.csv");
             
    val varCol_t = new Array[Integer](n);
    val varCol_c = new Array[Integer](n);
    for ( i <- 0 to n-1 ) {
      varCol_t(i) = sample(dist);
    }
    
    for ( i <- 0 to n-1 ) {
      varCol_c(i) = sample(dist);
    }
    
    
    // Add index to sample group for transactions
    val varColIndexed_t: Seq[(Integer, Int)] = varCol_t.zipWithIndex
    val varColIndexedDF_t = spark.sparkContext.parallelize(varColIndexed_t)
      .toDF("sample_group_t", "index");
   
    
    var varTable_t = transSample.join(varColIndexedDF_t,transSample("index") === varColIndexedDF_t("index"))
    varTable_t = varTable_t.drop("index")
    
    // Add index to sample group for customers
    val varColIndexed_c: Seq[(Integer, Int)] = varCol_c.zipWithIndex
    val varColIndexedDF_c = spark.sparkContext.parallelize(varColIndexed_c)
      .toDF("sample_group_c", "index");
   
    
    var varTable_c = custSample.join(varColIndexedDF_c,custSample("index") === varColIndexedDF_c("index"))
    varTable_c = varTable_c.drop("index")
   
    */
    
    // Join the two samples
    /*
    var varTable_j = varTable_c.join(varTable_t,varTable_c("cust_id") === varTable_t("cust_id"))
    println(varTable_j.count().toInt)
    
    val custUDF = udf(hij _)
    
    varTable_j = varTable_j.withColumn("tempCol", custUDF($"sample_group_c", $"sample_group_t"))
    
    varTable_j.show()
    
          val tableToSample = sample(dist)
      
      if ( tableToSample == 0 ) {
        val custSample = customers.sample(false, p);
        val custPartitionsNew = custSample.randomSplit(dataSplit)
        custPartitions = custPartitions ++ custPartitionsNew
        // Update the partition bounds
        i_lower = i_upper + 1
        i_upper = i_upper*2 + 1
        j_lower = 0
        j_upper = transPartitions.size - 1
      }
      else if ( tableToSample == 1 ) {
        val transSample = transactions.sample(false, p);
        val transPartitionsNew = transSample.randomSplit(dataSplit)
        transPartitions = transPartitions ++ transPartitionsNew
        // Update the partition bounds
        j_lower = j_upper + 1
        j_upper = j_upper*2 + 1
        i_lower = 0
        i_upper = custPartitions.size - 1
      }
        /*
   	 * Assign a sampling strategy to each table in the query.
   	 * There are 3 samplers at the moment: 
   	 * 1. univ - universe
   	 * 2. unif - uniform
   	 * 3. strat - stratified
     */
    val num_tables = join_inputs.size + 1
    // (table_name, sampler_type, sample_key)
    val table_sampler = scala.collection.mutable.Buffer[(String, String, String)]()
    // If only 2 tables, use universe sampling on both
    if (num_tables == 2){
      table_sampler.append((join_inputs(0)._1, "univ", join_inputs(0)._3))
      table_sampler.append((join_inputs(0)._2, "univ", join_inputs(0)._3))
    }
    // If 3 tables, use universe on first 2, and uniform on second
    else if (num_tables == 3){
      table_sampler.append((join_inputs(0)._1, "univ", join_inputs(0)._3))
      table_sampler.append((join_inputs(0)._2, "univ", join_inputs(0)._3))
      table_sampler.append((join_inputs(1)._2, "unif", join_inputs(1)._3))
    }
    // If 4 or more tables, look if a join sequence contains a subsequence of the same join keys
    else {
      var counts = scala.collection.mutable.Buffer[(String, Integer, Integer)]()
      var prevKey = ""
      var index = -1
      var currPos = 0
      
      // Calculate subsequence lengths
      for (ji <- join_inputs){
        val currKey = ji._3
        
        // If next join key is the same, update its count
        if (currKey == prevKey){
          counts(index) = (counts(index)._1, counts(index)._2 + 1, counts(index)._3)
          currPos = currPos + 1
          
        }
        // Otherwise, add new join key and initialise its count
        else {
          index = index + 1
          counts.append((ji._3, 1, currPos))
          currPos = currPos + 1
          
        }
        prevKey = currKey
        
      }
      
      println(counts.mkString("\n"))
      
      
      // Assign samplers
      breakable { for (tuple <- counts){
        val max_elem = counts.maxBy(_._2)
        val max_elem_index = counts.indexOf(max_elem)
        counts -= max_elem
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

        // Store (table, sampler)
        for (i <- 0 to subseq_key_count - 1){
          
          table_sampler.append((join_inputs(pos + i)._1, sampler, max_elem_key))
          
          if ( (pos + i) == join_inputs.size - 1 ){
            table_sampler.append((join_inputs(pos + i)._2, sampler, max_elem_key))
          }
        }
        
        if (counts.isEmpty)
          break
        
      }}
      
      val table_sampler2 = table_sampler.groupBy(v => (v._1))
      
      println(table_sampler2.mkString("\n"))
      
    }
      
    return null
    
    
    def ct = udf(() => {
    (java.time.LocalDateTime.now().minusSeconds(scala.util.Random.nextInt(100))).toString()
  })

  def stream_test(spark: SparkSession) {

    val customerSchema = StructType(
      StructField("c_custkey", LongType, true) ::
        StructField("c_name", StringType, true) ::
        StructField("c_address", StringType, true) ::
        StructField("c_nationkey", IntegerType, true) ::
        StructField("c_phone", StringType, true) ::
        StructField("c_acctbal", DecimalType(12, 2), true) ::
        StructField("c_mktsegment", StringType, true) ::
        StructField("c_comment", StringType, true) :: Nil);

    val orderSchema = StructType(
      StructField("o_orderkey", LongType, true) ::
        StructField("o_custkey", LongType, true) ::
        StructField("o_orderstatus", StringType, true) ::
        StructField("o_totalprice", DecimalType(12, 2), true) ::
        StructField("o_orderdate", DateType, true) ::
        StructField("o_orderpriority", StringType, true) ::
        StructField("o_clerk", StringType, true) ::
        StructField("o_shippriority", IntegerType, true) ::
        StructField("o_comment", StringType, true) :: Nil)

    val lineitemSchema = StructType(
      StructField("l_orderkey", LongType, true) :: // 0
        StructField("l_partkey", LongType, true) :: // 1
        StructField("l_suppkey", LongType, true) :: // 2
        StructField("l_linenumber", IntegerType, true) :: // 3
        StructField("l_quantity", DecimalType(12, 2), true) :: // 4
        StructField("l_extendedprice", DecimalType(12, 2), true) :: // 5
        StructField("l_discount", DecimalType(12, 2), true) :: // 6
        StructField("l_tax", DecimalType(12, 2), true) :: // 7
        StructField("l_returnflag", StringType, true) :: // 8
        StructField("l_linestatus", StringType, true) :: // 9
        StructField("l_shipdate", DateType, true) :: // 10
        StructField("l_commitdate", DateType, true) :: // 11
        StructField("l_receiptdate", DateType, true) :: // 12
        StructField("l_shipinstruct", StringType, true) :: // 13
        StructField("l_shipmode", StringType, true) :: // 14
        StructField("l_comment", StringType, true) :: // 15
        StructField("lsratio", DoubleType, true) :: Nil); // 16

    var lineitem_sample = spark.readStream.schema(lineitemSchema)
  //  .option("maxFilesPerTrigger", "1")

      .parquet("data_samples/lineitem/lineitem_unif.parquet")

      
    var customer_sample = spark.readStream.schema(customerSchema)
   // .option("maxFilesPerTrigger", "1")
 
      .parquet("data_samples/customer/customer_unif.parquet")


    var orders_sample = spark.readStream.schema(orderSchema)
 //   .option("maxFilesPerTrigger", "1")

      .parquet("data_samples/orders/orders_unif.parquet")


    import scala.concurrent.duration._
    val watermark = 160.seconds.toString()

    lineitem_sample = lineitem_sample.withColumn("timestamp", ct())

    lineitem_sample = lineitem_sample.withColumn("timestamp2", to_timestamp(col("timestamp")))
      .withWatermark("timestamp2", watermark)

    customer_sample = customer_sample.withColumn("timestamp", ct())

    customer_sample = customer_sample.withColumn("timestamp2", to_timestamp(col("timestamp")))
      .withWatermark("timestamp2", watermark)


    orders_sample = orders_sample.withColumn("timestamp", ct())

    orders_sample = orders_sample.withColumn("timestamp2", to_timestamp(col("timestamp")))
      .withWatermark("timestamp2", watermark)

    lineitem_sample.createOrReplaceTempView("lineitem_sample")
    customer_sample.createOrReplaceTempView("customer_sample")
    orders_sample.createOrReplaceTempView("orders_sample")
    
    
    val strDF = spark.sql("""select AVG(c_acctbal) from lineitem_sample
      join orders_sample on lineitem_sample.l_orderkey = orders_sample.o_orderkey 
      join customer_sample on orders_sample.o_custkey = customer_sample.c_custkey
      group by window(customer_sample.timestamp2, "160 seconds")
      """)

      
    /*
    val strDF = spark.sql("""
      select AVG(c_acctbal) from customer_sample join orders_sample on
      customer_sample.c_custkey = orders_sample.o_custkey
      group by window(customer_sample.timestamp2, "60 seconds")
      """)
      * 
      */
     

    /*
    val strDF = spark.sql("""
      select AVG(c_acctbal) from customer_sample
      group by window(customer_sample.timestamp2, "5 seconds")
      """)
      *
      */

    println("Started")
    val query = strDF.writeStream
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .format("console")
      .start()
     

    query.awaitTermination();

  }

  def run_query(spark: SparkSession, join_inputs: Array[(String, String, String)], agg: (String, String),
                samplers: Map[String, (String, String)]): DataFrame = {

    // var partitions = new ListBuffer[Array[DataFrame]]()
    var partitions = new ListBuffer[DataFrame]()
    val sizes = Array(6001215, 1500000, 150000)
    val np = 3.0
    val p = 0.1
    val dataSplit = Array.fill(np.toInt)(1 / np)
    val Samplers = new Samplers()
    // Number of tables
    val nt = join_inputs.size
    var partCount = Array(20140, 5018, 492)
    // All results computed so far
    var result = spark.emptyDataFrame
    val tol = 5.0
    var currentError = tol + 1
    var num_iter = 1
    val total_iter = math.pow(np, nt + 1)
    var indices = Array.fill(nt + 1)(1)

    // Sample and partition the tables
    for ((k, v) <- samplers) {
      val table = spark.sql("SELECT * FROM " + k)
      var sample = spark.emptyDataFrame
      val sample_type = v._1

      if (sample_type == "univ")
        //sample = Samplers.universe_sampling(table, k.take(1) + "_" + v._2, p);
        sample = Samplers.uniform_sampling(table, p).cache()
      else if (sample_type == "unif")
        sample = Samplers.uniform_sampling(table, p).cache()

      // dont split universe samples
      //   val columns = Seq("l_orderkey", "o_orderkey", "o_custkey", "c_custkey", "c_acctbal")
      //   val cols = columns.flatMap(c => Try(sample(c)).toOption)
      // val needed_columns: List[Column] = List(new Column(k.take(1) + "_" + v._2), new Column(agg._2))
      // sample = sample.select(needed_columns: _*)

      // sample = sample.select(cols: _*).coalesce(1)
      //  partitions.append(sample2)
      // TODO: Replace count with pre-determined number
      //partCount.append((sample.count()/np).toInt)
    }

    println("Sampling finished")

    //  import org.apache.spark.util.SizeEstimator
    //  println(SizeEstimator.estimate(partitions(2)))

    val query = """select /*+ BROADCAST(orders_sample) */ AVG(c_acctbal) from lineitem_sample
        join orders_sample on lineitem_sample.l_orderkey = orders_sample.o_orderkey
        join customer_sample on orders_sample.o_custkey = customer_sample.c_custkey
        """

    val startTime = System.nanoTime

    val dataFrame = spark.sql(query)
    dataFrame.show()

    val endTime = System.nanoTime

    val elapsedMs = (endTime - startTime) / 1e6d

    println("Time taken : " + elapsedMs + "ms")

    //&& currentError >= tol
    /*
    while ((num_iter <= total_iter - 1)) {
      partitions(0).createOrReplaceTempView("a")
      partitions(1).createOrReplaceTempView("b")
      partitions(2).createOrReplaceTempView("c")

      // Do the join

       var partRes = partitions(0)(indices(0)-1).join(partitions(1)(indices(1)-1),
          partitions(0)(indices(0)-1)(join_inputs(0)._1.take(1) + "_" + join_inputs(0)._3)
          === partitions(1)(indices(1)-1)((join_inputs(0)._2.take(1) + "_" + join_inputs(0)._3)))

       for ( i <- 2 to nt ){
         partRes = partRes.join(partitions(i)(indices(i)-1), partRes(join_inputs(i-1)._1.take(1) + "_" + join_inputs(i-1)._3)
          === partitions(i)(indices(i)-1)((join_inputs(i-1)._2.take(1) + "_" + join_inputs(i-1)._3)))
       }


       // Update number of tuples we sampled from each table
       for ( i <- 0 to nt ){
         nr_sampled(i) += partCount(i)
       }

       // Add new samples to old ones
       if ( num_iter == 1)
         result = partRes
       else
         result = result.union(partRes)

       // Evaluate new result

       var res = eval(result, join_inputs, agg, spark: SparkSession)
       currentError = res._1
       val est = res._2

       println("Result : " + est)
       println("Error : " + currentError)

    //   if (currentError <= tol)
      //      break

       // Update indices
       for ( i <- 1 to nt+1 ){
         // time for reset
         if ( num_iter % math.pow(np, nt + 2 - i) == 0)
           indices(i-1) = 1

         else if ( num_iter % math.pow(np, nt + 1 - i) == 0)
           indices(i-1) = indices(i-1) + 1
       }
       *


      num_iter += 1
    }
    *
    */

    return null
  }

    def eval(resDF: DataFrame, join_inputs: Array[(String, String, String)], agg: (String, String),
           spark: SparkSession): (Double, Double) = {
    /* Application parameters */
    import spark.implicits._
    val b = 40
    val alpha = 0.05
    // Number of tables
    val nt = join_inputs.size + 1
    // Subsample sizes
    var n = 0.0
    for (i <- 0 to nt - 1)
      n = n + nr_sampled(i)

    var ns = math.pow(n, 0.5)

    // Create distributions
    for (i <- 0 to nt - 1)
      distributions.append(Map(0 -> (nr_sampled(i) - b * ns) / nr_sampled(i)))

    distributions(0) += (1 -> (nr_sampled(0) - b * ns) / nr_sampled(0))
    // Populate distributions for each table

    for (i <- 0 to nt - 1) {
      for (j <- 1 to b)
        distributions(i) += (j -> (ns / nr_sampled(i)))
    }

    // Define udf to assing subsample groups to tuples
    val custUDF = udf(h _)

    // Assign sid's to result tuples
    var result = resDF.withColumn("sid", custUDF())
    result = result.where("sid != 0")
    result.cache()

    // resDF.createOrReplaceTempView("result")

    //val dresult=spark.sql(query)

    val groupedBySid = result.groupBy("sid")
    var subsample_est = Array((0, 0.0))
    var est = 0.0

    // TODO: update groups, instead of recalculating every time
    if (agg._1 == "AVG") {
      //  groupedBySid.agg(avg(agg._2)).rdd.map(row => (row.getInt(0), row.getDecimal(1).doubleValue()))
      //   .collect()
      result.agg(avg(agg._2)).limit(1).collect()

      //println(rt.collect.map(_.toSeq).flatten)

    } else if (agg._1 == "SUM") {
      subsample_est = groupedBySid.agg(sum(agg._2)).rdd.map(row => (row.getInt(0), row.getDecimal(1).doubleValue()))
        .collect()
      est = result.agg(sum(agg._2)).rdd.map(row => (row.getDecimal(0))).collect()(0).doubleValue()
    }

    // verify confidence bounds
    // val cb = get_conf_bounds(alpha, est, subsample_est, n.toInt, ns)
    // val error = ((cb._1 - cb._2)/2) / (((cb._1 + cb._2)/2)) * 100

    result.unpersist()
    distributions.clear()

    return (0.0, 0.0)

  }

    def h(n: Integer, nt: Integer, b: Integer ) = udf{
    (x: Integer) =>

    val samples = new Array[Int](nt)
    val start = 1
    val rnd = new scala.util.Random

    for (i <- 0 until nt - 1) {
      val ns = Math.pow(nr_sampled(i), 0.5)
      val end = (ns / 10).toInt
      val r = start + rnd.nextInt((end - start) + 1)

      if (r <= b)
        samples(i) = 0
      else
        samples(i) = r
    }

    if (samples.contains(0))
      return 0
    else {
      var h = 1.0
      for (i <- 0 to nt - 1) {
        h += Math.floor((samples(i)) / math.sqrt(b)) * math.pow(math.sqrt(b), nt - 1 - i)
      }
      return h.toInt
    }
  }

    final def sample[A](dist: scala.collection.mutable.Map[A, Double]): A = {
    val p = scala.util.Random.nextDouble * dist.values.sum
    val it = dist.iterator
    var accum = 0.0
    while (it.hasNext) {
      val (item, itemProb) = it.next
      accum += itemProb
      if (accum >= p)
        return item // return so that we don't have to search through the whole distribution
    }
    sys.error(f"Sampling error") // needed so it will compile
  }

  val query_v1 = """select AVG(c_acctbal) from parquet.`data_samples_sf10/lineitem/lineitem_unif.parquet` as lineitem_sample
      join parquet.`data_samples_sf10/orders/orders_unif.parquet` as orders_sample
      on lineitem_sample.l_orderkey = orders_sample.o_orderkey
      join parquet.`data_samples_sf10/customer/customer_unif.parquet` as customer_sample
      on orders_sample.o_custkey = customer_sample.c_custkey
      """


            partial.createOrReplaceTempView("partial")

      val testQuery =
        """select * from (select `avg(o_totalprice)` - (select avg(`avg(o_totalprice)`) from partial) as diff
          |from partial order by diff asc)""".stripMargin

      val DF = spark.sql(testQuery).show()


     */

  /*
  def addColumnIndex(df: DataFrame, spark: SparkSession): DataFrame = {
    return spark.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map {
        case (row, index) => Row.fromSeq(row.toSeq :+ index)
      },
      // Create schema for index column
      StructType(df.schema.fields :+ StructField("index", LongType, false)))
  }

   */

}



