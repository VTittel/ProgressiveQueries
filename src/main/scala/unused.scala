

object unused {
 // Create variational table for transactions and customers
  
    /*
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
    */
}