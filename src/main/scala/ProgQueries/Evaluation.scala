package ProgQueries

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{array, avg, col, lit, struct, sum, udf}

import scala.collection.mutable
import scala.collection.mutable.Map
/*
Result evaluation based on VerdictDB paper

https://arxiv.org/pdf/1804.00770.pdf
 */
class Evaluation {

  /*
  Function to assign subsamples to tuples
   */

  def assign_subsamples(spark: SparkSession, table: DataFrame, tableName: String, n: Double, b: Integer): DataFrame = {

    val rnd = new scala.util.Random
    val start = 1
    val ns = Math.pow(n, 0.5)
    val end = (ns).toInt

    val assign_subsample = udf(() => {
      val r = start + rnd.nextInt((end - start) + 1)

      if (r <= b)
        r.toString;
      else
        "0";
    })

    val newTable = table.withColumn(tableName.take(1) + "_sid", assign_subsample())
    return newTable
  }


  /*
  Function to evaluate (report error metrics) single table queries
   */
  def eval_single_table(resultDF: DataFrame, params: Map[String, String], agg: (String, String), sf: Integer): Map[String, String] = {
    val alpha = params("alpha").toDouble

    // Subsample sizes
    val n = resultDF.count()
    val ns = math.pow(n, 0.5)
    var est: BigDecimal = BigDecimal("0.0")
    val subsamples_est: Array[BigDecimal] = resultDF.rdd.map(row => (BigDecimal.valueOf(row.getDecimal(0).doubleValue())))
      .collect()

    agg._1 match {
      case "avg" => {
        est = resultDF.agg(avg(agg._1 + "(" + agg._2 + ")")).rdd.map(row => (row.getDecimal(0))).collect()(0)
          .setScale(2, BigDecimal.RoundingMode.HALF_UP)
      }
      case "sum" => {
        est = resultDF.agg(avg(agg._1 + "(" + agg._2 + ")")).rdd.map(row => (row.getDecimal(0))).collect()(0)
          .setScale(2, BigDecimal.RoundingMode.HALF_UP)

      }
    }


    val cb = get_conf_bounds(alpha, est, subsamples_est, n.toInt, ns)
    val error = ((cb._1 - cb._2)/2) / (((cb._1 + cb._2)/2)) * 100

    return mutable.Map("error" -> error.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString,
      "ci_low" -> cb._2.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString,
      "ci_high" -> cb._1.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString)

  }

  /*
  Function to evaluate the accuracy of a given partial result for a join query
   */
  /*

  def eval_join(resultDF: DataFrame, join_inputs: Array[(String, String, String)], agg: (String, String),
           spark: SparkSession): (Double, Double) = {
    /* Application parameters */
    val alpha = 0.05
    // Subsample sizes
    val n = resultDF.count()
    val b: Integer = 100
    val ns = math.pow(n, 0.5)

    // Define udf to assing subsample groups to tuples
    val sidUDF = udf(h _)

    // Assign sid's to result tuples
    val sidColumns = resultDF.schema.fieldNames.filter( col => col.contains("_sid"))
    var result = resultDF.withColumn("sid", sidUDF(lit(b), struct(resultDF.columns map col: _*),
      array(sidColumns.map(lit(_)): _*)))
    result.cache()  // needed, otherwise filter doesnt work
    result = result.where("sid != 0")

    val groupedBySid = result.groupBy("sid")
    var subsample_est = Array(0.0)
    var est = 0.0

    // TODO: update groups, instead of recalculating every time
    if (agg._1 == "AVG") {
      subsample_est = groupedBySid.agg(avg(agg._2)).rdd.map(row => (row.getInt(0), row.getDecimal(1).doubleValue()))
        .collect()
      est = result.agg(avg(agg._2)).rdd.map(row => (row.getDecimal(0))).collect()(0).doubleValue()
    } else if (agg._1 == "SUM") {
      subsample_est = groupedBySid.agg(sum(agg._2)).rdd.map(row => (row.getInt(0), row.getDecimal(1).doubleValue()))
        .collect()
      est = result.agg(sum(agg._2)).rdd.map(row => (row.getDecimal(0))).collect()(0).doubleValue()
    }

    // TODO: verify confidence bounds
    val cb = get_conf_bounds(alpha, est, subsample_est, n.toInt, ns)
    val error = ((cb._1 - cb._2)/2) / (((cb._1 + cb._2)/2)) * 100

    result.unpersist()

    return (error, est)
  }

   */


  def get_conf_bounds(alpha: Double, est: BigDecimal, subsamples_est: Array[BigDecimal],
                      n: Integer, ns: Double): (BigDecimal, BigDecimal) = {

    val diffs = new Array[BigDecimal](subsamples_est.length)

    for (i <- subsamples_est.indices) {
      diffs(i) = subsamples_est(i) - est
    }

    val lower_q = get_quantile(diffs, alpha)
    val upper_q = get_quantile(diffs, 1 - alpha)
    val lower_bound = est - lower_q * (math.sqrt(ns / n))
    val upper_bound = est - upper_q * (math.sqrt(ns / n))

    return (lower_bound, upper_bound)
  }


  def get_quantile(diffs: Array[BigDecimal], quantile: Double): BigDecimal = {

    val diffs2 = diffs.sorted
    val index = (Math.ceil(quantile / 1.0 * diffs2.length)).toInt
    return diffs2(index - 1);
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

}
