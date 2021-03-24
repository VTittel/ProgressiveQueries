package FileHandlers

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
 * Main for running random queries
 * Par. 1 : DROP & LOAD (0 do not drop & load, 1 for drop and load, 2 for drop and load samples)
 */
object CreateTPCHparquet {

  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder()
      .appName("Spark SQL basic example")
      //      .config("spark.master", "spark://mcs-computeA002:7077")
      .config("spark.master", "local[4]")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR");
    sparkSession.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkSession.conf.set("spark.sql.codegen.wholeStage", false);
    // benchmark (tpch or tpcds
    val bench = "tpch" //args(0);
    // Scaling factor (used in file paths)
    val sf = 1 //args(1);
    // hdfs or local
    val hdfsOrLocal = "local" //args(2);
    var format = "";
    if (bench.contains("tpch")) {
      format = "csv";
    } else if (bench.contains("tpcds")) {
      format = "parquet"
    }

    var PARENT_DIR = ""

    val DATA_DIR = "tables/";
    // ============================================================> NATION
    val nationSchema = StructType(
      StructField("n_nationkey", LongType, true) ::
        StructField("n_name", StringType, true) ::
        StructField("n_regionkey", LongType, true) ::
        StructField("n_comment", StringType, true) :: Nil)
    //        val nation = sparkSession.read.parquet(DATA_DIR + "nation.parquet");
    val nation = sparkSession.sqlContext.read.format("csv").schema(nationSchema).option("nullValue", "null").option("delimiter", "|").load(DATA_DIR + "nation.tbl");
    sparkSession.sqlContext.createDataFrame(nation.rdd, nationSchema).createOrReplaceTempView("nation");
    nation.write.format("parquet").save(PARENT_DIR + "data_parquet/nation.parquet");
    // ============================================================> CUSTOMER
    val customerSchema = StructType(
      StructField("c_custkey", LongType, true) ::
        StructField("c_name", StringType, true) ::
        StructField("c_address", StringType, true) ::
        StructField("c_nationkey", IntegerType, true) ::
        StructField("c_phone", StringType, true) ::
        StructField("c_acctbal", DecimalType(12, 2), true) ::
        StructField("c_mktsegment", StringType, true) ::
        StructField("c_comment", StringType, true) :: Nil);
    val customer = sparkSession.sqlContext.read.format("csv").schema(customerSchema).option("nullValue", "null").option("delimiter", "|").load(DATA_DIR + "customer.tbl");
    sparkSession.sqlContext.createDataFrame(customer.rdd, customerSchema).createOrReplaceTempView("customer");
    customer.write.format("parquet").save(PARENT_DIR + "data_parquet/customer.parquet");
    // ============================================================> LINEITEM
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
    val lineitem = sparkSession.sqlContext.read.format("csv").schema(lineitemSchema).option("nullValue", "null").option("delimiter", "|").load(DATA_DIR + "lineitem.tbl");
    sparkSession.sqlContext.createDataFrame(lineitem.rdd, lineitemSchema).createOrReplaceTempView("lineitem");
    lineitem.write.format("parquet").save(PARENT_DIR + "data_parquet/lineitem.parquet");
    // ============================================================> ORDER
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
    val order = sparkSession.sqlContext.read.format("csv").schema(orderSchema).option("nullValue", "null").option("delimiter", "|").load(DATA_DIR + "orders.tbl");
    sparkSession.sqlContext.createDataFrame(order.rdd, orderSchema).createOrReplaceTempView("orders");
    order.write.format("parquet").save(PARENT_DIR + "data_parquet/order.parquet");
    // ============================================================> PART
    val partSchema = StructType(
      StructField("p_partkey", LongType, true) ::
        StructField("p_name", StringType, true) ::
        StructField("p_mfgr", StringType, true) ::
        StructField("p_brand", StringType, true) ::
        StructField("p_type", StringType, true) ::
        StructField("p_size", IntegerType, true) ::
        StructField("p_container", StringType, true) ::
        StructField("p_retailprice", DecimalType(12, 2), true) ::
        StructField("p_comment", StringType, true) :: Nil)
    val part = sparkSession.sqlContext.read.format("csv").schema(partSchema).option("nullValue", "null").option("delimiter", "|").load(DATA_DIR + "part.tbl");
    sparkSession.sqlContext.createDataFrame(part.rdd, partSchema).createOrReplaceTempView("part");
    part.write.format("parquet").save(PARENT_DIR + "data_parquet/part.parquet");
    // ============================================================> PARTSUPP
    val partsuppSchema = StructType(
      StructField("ps_partkey", LongType, true) ::
        StructField("ps_suppkey", LongType, true) ::
        StructField("ps_availqty", IntegerType, true) ::
        StructField("ps_supplycost", DecimalType(12, 2), true) ::
        StructField("ps_comment", StringType, true) :: Nil)
    val partsupp = sparkSession.sqlContext.read.format("csv").schema(partsuppSchema).option("nullValue", "null").option("delimiter", "|").load(DATA_DIR + "partsupp.tbl");
    sparkSession.sqlContext.createDataFrame(partsupp.rdd, partsuppSchema).createOrReplaceTempView("partsupp");
    partsupp.write.format("parquet").save(PARENT_DIR + "data_parquet/partsupp.parquet");
    // ============================================================> REGION
    val regionSchema = StructType(
      StructField("r_regionkey", LongType, true) ::
        StructField("r_name", StringType, true) ::
        StructField("r_comment", StringType, true) :: Nil)
    val region = sparkSession.sqlContext.read.format("csv").schema(regionSchema).option("nullValue", "null").option("delimiter", "|").load(DATA_DIR + "region.tbl");
    sparkSession.sqlContext.createDataFrame(region.rdd, regionSchema).createOrReplaceTempView("region");
    region.write.format("parquet").save(PARENT_DIR + "data_parquet/region.parquet");
    // ============================================================> SUPPLIER
    val supplierSchema = StructType(
      StructField("s_suppkey", LongType, true) ::
        StructField("s_name", StringType, true) ::
        StructField("s_address", StringType, true) ::
        StructField("s_nationkey", LongType, true) ::
        StructField("s_phone", StringType, true) ::
        StructField("s_acctbal", DecimalType(12, 2), true) ::
        StructField("s_comment", StringType, true) :: Nil)
    val supplier = sparkSession.sqlContext.read.format("csv").schema(supplierSchema).option("nullValue", "null").option("delimiter", "|").load(DATA_DIR + "supplier.tbl");
    sparkSession.sqlContext.createDataFrame(supplier.rdd, supplierSchema).createOrReplaceTempView("supplier");
    supplier.write.format("parquet").save(PARENT_DIR + "data_parquet/supplier.parquet");
  }
}
