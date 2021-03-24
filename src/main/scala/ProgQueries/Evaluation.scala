package ProgQueries

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, udf}

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
  def evaluatePartialResult(resultDF: DataFrame, params: Map[String, String],
                            aggregates: ArrayBuffer[(String, String, String)],
                            sf: Double): ArrayBuffer[Map[String, String]] = {

    val alpha = params("alpha").toDouble
    val subsampPerAgg = mutable.Map[String, Array[Seq[String]]]()
    var n = 0
    val errorsArray = ArrayBuffer[mutable.Map[String, String]]()

    // TODO: Handle aliases for aggregates
    val aggStrings = aggregates.map(t => t._3)
    val projections: Seq[String] = (resultDF.schema.fieldNames.toSet).filterNot(aggStrings.toSet).map(i => i.toString).toSeq
    // Calculate subsamples for each aggregate and store in an array
    for (agg <- aggStrings){
      val subsamples = resultDF.select(agg, projections: _*).rdd.map(row => row.toSeq.map(_.toString))
        .collect()

      n = subsamples.length

      subsampPerAgg += ((agg, subsamples))
    }

    val ns = math.pow(n, 0.5)

    for (agg <- aggStrings){
      val grouped = subsampPerAgg(agg).groupBy(_.tail).map{case (k,v) => k -> v.map(_.head.toDouble)}
      var aggValue = 0.0
      grouped.keys.foreach( (groupKeys) => {

        if (agg.contains("avg")) {
          aggValue = grouped(groupKeys).sum / grouped(groupKeys).length
        } else if (agg.contains("sum")){
          aggValue = grouped(groupKeys).sum * sf
        } else if (agg.contains("count")){
          aggValue = grouped(groupKeys).length * sf
        }

        val cb = getConfBounds(alpha, aggValue, grouped(groupKeys), n, ns)
        val error = ((cb._1 - cb._2) / 2) / (((cb._1 + cb._2) / 2)) * 100

        errorsArray.append(mutable.Map("agg" -> agg,
          "group" -> groupKeys.mkString(","),
          "error" -> BigDecimal(error).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString,
          "ci_low" -> BigDecimal(cb._2).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString,
          "ci_high" -> BigDecimal(cb._1).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString,
          "est" -> BigDecimal(aggValue).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString))
      })

    }

    return errorsArray
  }


  def getConfBounds(alpha: Double, est: Double, subsamples_est: Array[Double],
                      n: Integer, ns: Double): (Double, Double) = {

    val diffs = new Array[Double](subsamples_est.length)

    for (i <- subsamples_est.indices) {
      diffs(i) = subsamples_est(i) - est
    }

    val lowerQ = getQuantile(diffs, alpha)
    val upperQ = getQuantile(diffs, 1 - alpha)
    val lowerBound = est - lowerQ * (math.sqrt(ns / n))
    val upperBound = est - upperQ * (math.sqrt(ns / n))

    return (lowerBound, upperBound)
  }


  def getQuantile(diffs: Array[Double], quantile: Double): Double = {

    val diffs2 = diffs.sorted
    val index = (Math.ceil(quantile / 1.0 * diffs2.length)).toInt
    return diffs2(index - 1);
  }


}
