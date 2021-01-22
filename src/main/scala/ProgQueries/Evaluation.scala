package ProgQueries

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.udf

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map}
/*
Result evaluation based on VerdictDB paper

https://arxiv.org/pdf/1804.00770.pdf
 */
class Evaluation extends Serializable {

  /*
  Function to assign subsamples to tuples
   */

  def assignSubsamples(spark: SparkSession, table: DataFrame, tableName: String, n: Double, b: Integer): DataFrame = {

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
  def evaluatePartialResult(resultDF: DataFrame, resultDFAggregated: DataFrame, params: Map[String, String],
                            aggregates: ArrayBuffer[(String, String)],
                            sf: Integer): ArrayBuffer[Map[String, String]] = {

    val alpha = params("alpha").toDouble
    val subsampPerAgg = mutable.Map[String, Array[BigDecimal]]()
    var n = 0
    val errorsArray = ArrayBuffer[mutable.Map[String, String]]()

    // Calculate subsamples for each aggregate and store in an array
    for (agg <- aggregates){

      val aggString = agg._1 + "(" + agg._2 + ")"

      val subsamples = resultDF.select(aggString).rdd.map(row => (BigDecimal.valueOf(row.getDecimal(0).doubleValue())))
        .collect()

      n = subsamples.length

      subsampPerAgg += ((aggString, subsamples))
    }

    val ns = math.pow(n, 0.5)
    // Number of groups created by group by
    val resultAggregated = resultDFAggregated.collect()

    // For each group, for each aggregate
    for (row <- resultAggregated){
      for (agg <- aggregates){
        val aggString = agg._1 + "(" + agg._2 + ")"
        val aggValue = row.getAs(aggString).toString.toDouble
        // TODO: add group by columns to result
        val cb = getConfBounds(alpha, aggValue, subsampPerAgg(aggString), n, ns)
        val error = ((cb._1 - cb._2)/2) / (((cb._1 + cb._2)/2)) * 100

        errorsArray.append(mutable.Map("error" -> error.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString,
          "ci_low" -> cb._2.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString,
          "ci_high" -> cb._1.setScale(2, BigDecimal.RoundingMode.HALF_UP).toString,
          "est" -> aggValue.toString))
      }
    }

    return errorsArray
  }


  def getConfBounds(alpha: Double, est: BigDecimal, subsamples_est: Array[BigDecimal],
                      n: Integer, ns: Double): (BigDecimal, BigDecimal) = {

    val diffs = new Array[BigDecimal](subsamples_est.length)

    for (i <- subsamples_est.indices) {
      diffs(i) = subsamples_est(i) - est
    }

    val lower_q = getQuantile(diffs, alpha)
    val upper_q = getQuantile(diffs, 1 - alpha)
    val lower_bound = est - lower_q * (math.sqrt(ns / n))
    val upper_bound = est - upper_q * (math.sqrt(ns / n))

    return (lower_bound, upper_bound)
  }


  def getQuantile(diffs: Array[BigDecimal], quantile: Double): BigDecimal = {

    val diffs2 = diffs.sorted
    val index = (Math.ceil(quantile / 1.0 * diffs2.length)).toInt
    return diffs2(index - 1);
  }


}
