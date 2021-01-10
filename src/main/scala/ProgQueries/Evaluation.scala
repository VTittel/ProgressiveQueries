package ProgQueries

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{array, avg, col, lit, struct, sum, udf}

import scala.collection.mutable
import scala.collection.mutable.Map
/*
Result evaluation based on VerdictDB paper

https://arxiv.org/pdf/1804.00770.pdf
 */
class Evaluation extends Serializable {

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
  Function to evaluate (report error metrics) single and multi table queries
   */
  def evaluatePartialResult(resultDF: DataFrame, params: Map[String, String], agg: (String, String), sf: Integer): Map[String, String] = {
    val alpha = params("alpha").toDouble
   // resultDF.cache()
    // Subsample sizes
    var est: BigDecimal = BigDecimal("0.0")
    val subsamples_est: Array[BigDecimal] = resultDF.rdd.map(row => (BigDecimal.valueOf(row.getDecimal(0).doubleValue())))
      .collect()

    val n = subsamples_est.length
    val ns = math.pow(n, 0.5)

    agg._1 match {
      case "avg" => {
        est = subsamples_est.sum/subsamples_est.length
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
      "ci_high" -> cb._1.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString,
      "est" -> est.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString)

  }


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


}
