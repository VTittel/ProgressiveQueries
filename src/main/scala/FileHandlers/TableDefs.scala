package FileHandlers

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object TableDefs {

  def load_tpch_tables(sparkSession: SparkSession, DATA_DIR: String) = {
    // TPC-H tables
    // ============================================================> NATION
    val nationSchema = StructType(
      StructField("n_nationkey", LongType, true) ::
        StructField("n_name", StringType, true) ::
        StructField("n_regionkey", LongType, true) ::
        StructField("n_comment", StringType, true) :: Nil)
    val nation = sparkSession.read.parquet(DATA_DIR + "nation");
    sparkSession.sqlContext.createDataFrame(nation.rdd, nationSchema).createOrReplaceTempView("nation");
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
    val customer = sparkSession.read.parquet(DATA_DIR + "customer");
    sparkSession.sqlContext.createDataFrame(customer.rdd, customerSchema).createOrReplaceTempView("customer");
    // ============================================================> LINEITEM
    val lineitemSampledSchema = StructType(
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
    val lineitem = sparkSession.read.parquet(DATA_DIR + "lineitem");
    sparkSession.sqlContext.createDataFrame(lineitem.rdd, lineitemSampledSchema).createOrReplaceTempView("lineitem");
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
    val order = sparkSession.read.parquet(DATA_DIR + "order");
    sparkSession.sqlContext.createDataFrame(order.rdd, orderSchema).createOrReplaceTempView("order");
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
    val part = sparkSession.read.parquet(DATA_DIR + "part");
    sparkSession.sqlContext.createDataFrame(part.rdd, partSchema).createOrReplaceTempView("part")
    // ============================================================> PARTSUPP
    val partsuppSchema = StructType(
      StructField("ps_partkey", LongType, true) ::
        StructField("ps_suppkey", LongType, true) ::
        StructField("ps_availqty", IntegerType, true) ::
        StructField("ps_supplycost", DecimalType(12, 2), true) ::
        StructField("ps_comment", StringType, true) :: Nil)
    val partsupp = sparkSession.read.parquet(DATA_DIR + "partsupp");
    sparkSession.sqlContext.createDataFrame(partsupp.rdd, partsuppSchema).createOrReplaceTempView("partsupp");
    // ============================================================> REGION
    val regionSchema = StructType(
      StructField("r_regionkey", LongType, true) ::
        StructField("r_name", StringType, true) ::
        StructField("r_comment", StringType, true) :: Nil)
    val region = sparkSession.read.parquet(DATA_DIR + "region");
    sparkSession.sqlContext.createDataFrame(region.rdd, regionSchema).createOrReplaceTempView("region");
    // ============================================================> SUPPLIER
    val supplierSchema = StructType(
      StructField("s_suppkey", LongType, true) ::
        StructField("s_name", StringType, true) ::
        StructField("s_address", StringType, true) ::
        StructField("s_nationkey", LongType, true) ::
        StructField("s_phone", StringType, true) ::
        StructField("s_acctbal", DecimalType(12, 2), true) ::
        StructField("s_comment", StringType, true) :: Nil)

    val supplier = sparkSession.read.parquet(DATA_DIR + "supplier");
    sparkSession.sqlContext.createDataFrame(supplier.rdd, supplierSchema).createOrReplaceTempView("supplier");
  }

}
