import org.apache.spark.sql.functions._
import scala.util.control.Breaks._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window

object entryPoint {
  
  val dist_c = scala.collection.mutable.Map[Integer, Double]()
  val dist_t = scala.collection.mutable.Map[Integer, Double]()
  def main (args: Array[String]): Unit = {
  
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
       .config("spark.master", "local")
      .getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")
    
    // Load external datasets
    val transactions = spark.read.format("csv")
                .option("header", "true").load("Transactions.csv");
    val customers = spark.read.format("csv")
                .option("header", "true").load("Customers.csv");
    
    val t_count = transactions.count()
    val c_count = customers.count()
    /* Application parameters */
    val b = 40
    val p = 0.1
    val numPart = 3
    val dataSplit = Array.fill(numPart)(1.0/numPart.toDouble)
    val n_c = (c_count * 0.1).toInt
    val n_t = (t_count * 0.1).toInt
    val ns_c = math.pow(n_c, 0.5);
    val ns_t = math.pow(n_t, 0.5);
    
    println(n_c, n_t, ns_c, ns_t)
    
    
    //TODO : fix sampling function
    // Define udf to assing subsample groups to tuples
    val custUDF = udf(hij _)
    
    // Create distributions
    dist_c(0) = (n_c-b*ns_c)/n_c;
    dist_t(1) = (n_t-b*ns_t)/n_t;
    
    for( i <- 1 to b ) {
      dist_c(i) = ns_c / n_c;
      dist_t(i) = ns_t / n_t;
    }
    
    // Pick which table to sample based on tables size - smaller tables have bigger chance to be sampled
    val dist = scala.collection.mutable.Map[Integer, Double]()
    val total_count = t_count + c_count
    dist(0) = (total_count - c_count) / total_count.toDouble
    dist(1) = (total_count - t_count) / total_count.toDouble

    var result = spark.emptyDataFrame
    val tol = 5.0
    var currentError = tol + 1
    var i_lower = 0
    var i_upper = numPart - 1
    var j_lower = 0
    var j_upper = numPart - 1
    
    // Sample the join tables
    // TODO: Decide which tables to sample
    val custSample = customers.sample(false, p);
    val transSample = transactions.sample(false, p);
    // Partition the tables
    var custPartitions = custSample.randomSplit(dataSplit)
    var transPartitions = transSample.randomSplit(dataSplit)
    
    while (currentError >= tol){
      
      for ( i <- i_lower to i_upper ){
        for ( j <- j_lower to j_upper ) {
          var partRes = custPartitions(i).join(transPartitions(j),
          custPartitions(i)("cust_id") === transPartitions(j)("cust_id"))
          
          // Assign sid's to result tuples
          partRes = partRes.withColumn("sid", custUDF()).cache()
          partRes = partRes.where("sid != 0")
          
          // Add new samples to old ones
          if ( i==0 && j==0)
            result = partRes
          else
            result = result.union(partRes)
          
            
          val n = result.count().toInt
          
          val groupedBySid = result.groupBy("sid")
          val subsample_est = groupedBySid.agg(avg("Tax"))
            .rdd.map(row => (row.getInt(0), row.getDouble(1))).collect()
            
          val alpha = 0.05
          val sampAvg = result.agg(avg("Tax")).rdd.map(row => (row.getDouble(0))).collect()(0)
          val cb = get_conf_bounds(0.05, sampAvg, subsample_est, n, ns_t)
          var error = get_error(subsample_est)
          error = (100/sampAvg)*(1.96*(error/math.sqrt(n.toDouble)))
          println("The sample average is " + sampAvg + 
              " with a margin of error of +=" + 
              BigDecimal(error).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble + "%")
          
          val cb_error = error * (sampAvg/100)
          println("The confidence bound is : [" + (sampAvg - cb_error) ,(sampAvg + cb_error) + "]")
          
          partRes.unpersist()
          
          if (error <= tol) {
            break
          }
          else
            currentError = error
            
          
          // TODO: break loop early once error is reached
          
          /* 
           * 2-level sampling : for each table
           * 1. Stratified sampling x% of table
           * 2. First k partitions contain stratified sample
           */
        }
          
      }
      
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
      
      
    }
    
    var realRes = customers.join(transactions,
          customers("cust_id") === transactions("cust_id")).agg(avg("Tax")).show()
    
    //varTable_j.groupBy("sid").agg(avg("Tax").as("avg_tax")).agg(avg("avg_tax")).show()
    
    println("Program finished");
  }
  
  
  
  final def sample[A](dist: scala.collection.mutable.Map[A, Double]): A = {
    val p = scala.util.Random.nextDouble * dist.values.sum
    val it = dist.iterator
    var accum = 0.0
    while (it.hasNext) {
      val (item, itemProb) = it.next
      accum += itemProb
      if (accum >= p)
        return item  // return so that we don't have to search through the whole distribution
    }
    sys.error(f"Sampling error")  // needed so it will compile
  }
  
  
  def hij() : Int = {
      val i = sample(dist_c)
      val j = sample(dist_t)
 
      if (i == 0 || j == 0)
        return 0
      else {
        val b = 20
        var h = Math.floor((i-1)/Math.sqrt(b))* Math.sqrt(b) + Math.floor((j-1)/Math.sqrt(b)) + 1;
        return h.toInt
      }
   }
  
   def get_conf_bounds(alpha: Double, samp_est: Double, est_arr: Array[(Int, Double)], 
       n: Integer, ns: Double): (Double, Double) = {
     
     var diffs = new Array[Double](est_arr.size)
     
     for( i <- 0 to est_arr.size - 1){
        diffs(i) = est_arr(i)._2 - samp_est
     }
     
     val lower_q = get_quantile(diffs, alpha)
     val upper_q = get_quantile(diffs, 1 - alpha)
     val lower_bound = samp_est - lower_q * (math.sqrt(ns/n))
     val upper_bound = samp_est - upper_q * (math.sqrt(ns/n))
     
     return (lower_bound, upper_bound)
   }
   
   
   def get_quantile(diffs: Array[Double], quantile: Double): Double = {
     
     val diffs2 = diffs.sorted
     val index = (Math.ceil(quantile / 1.0 * diffs2.size)).toInt
     return diffs2(index-1);
   }
   
   
   def get_error(est_arr: Array[(Int, Double)]): Double = {
    
     val n = est_arr.size
     var st_error = 0.0
     
     // Calculate mean of subsample estimates
     var est_avg = 0.0
    
     for (se <- est_arr){
       est_avg += se._2
     }
     
     est_avg = est_avg / n
     
     // Standard error
     for( i <- 0 to n - 1){
        st_error += math.pow(est_arr(i)._2 - est_avg, 2)
     }
     
     st_error = math.sqrt(st_error / n)
     
     return st_error
     
   }
   
  
  
}