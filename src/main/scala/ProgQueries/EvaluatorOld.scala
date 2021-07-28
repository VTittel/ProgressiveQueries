package ProgQueries

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, udf}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}
/*
Result evaluation based on VerdictDB paper

https://arxiv.org/pdf/1804.00770.pdf
 */
class EvaluatorOld extends Serializable {

  /*
  Function to evaluate (report error metrics) single and multi table queries
   */

  // TODO: Fix scaling
  def evaluatePartialResult(resultDF: DataFrame, params: Map[String, String],
                            aggregates: ArrayBuffer[(String, String, String)],
                            sf: Double): ArrayBuffer[Map[String, String]] = {

    val alpha = params("alpha").toDouble
    val subsampPerAgg = mutable.Map[String, Array[Seq[String]]]()
    var n = 0
    val errorsArray = ArrayBuffer[mutable.Map[String, String]]()

    // TODO: Handle aliases for aggregates
    val aggStrings = aggregates.map(t => t._3)
    var projections: Seq[String] = (resultDF.schema.fieldNames.toSet).filterNot(aggStrings.toSet).map(i => i.toString).toSeq
    if (!projections.contains("sid"))
      projections = projections :+ "sid"

    // Calculate subsamples for each aggregate and store in an array
    // length of row - hack, needed for subsample extraction
    var rowLen = 0

    for (agg <- aggStrings){
      val subsamples = resultDF.select(agg, projections: _*).rdd.map(row => row.toSeq.map(_.toString))
        .collect()

      n += subsamples.length
      rowLen = subsamples.head.length

      subsampPerAgg += (agg -> subsamples)
    }

    val subsampPerAggNoSid = subsampPerAgg.map{case (k,v) => k -> v.map(x => x.dropRight(1))}

    println(subsampPerAgg("sum_qty").mkString(","))

    println("******************************************")

    val ns = math.pow(n, 0.5)

    aggStrings -= "sid"

    for (agg <- aggStrings){
      val grouped = subsampPerAggNoSid(agg).groupBy(_.tail).map{case (k,v) => k -> v.map(_.head.toDouble)}

      /*
      println(grouped.keys.head)
      println(grouped(grouped.keys.head).mkString("\n"))
      println("***********************************************************")

       */
      var aggValue = 0.0

      grouped.keys.foreach( groupKeys => {
        // maps group to (estimate, subsample)
        val groupedSid = subsampPerAgg(agg).groupBy(_.slice(1, rowLen-1)).map{case (k,v) =>
          k -> v.map(x => (x.head.toDouble, x.last.toDouble))}

        // filter out groups with sid = 0
        val estimatesWithSid = groupedSid(groupKeys)
        val estimates = estimatesWithSid.filter(_._2 > 0.0).map(x => x._1)
        var estimatesScaled = Array()

        if (agg.contains("avg")) {
          aggValue = grouped(groupKeys).sum / grouped(groupKeys).length
        } else if (agg.contains("sum")){
          aggValue = grouped(groupKeys).sum * sf
        } else if (agg.contains("count")){
          aggValue = grouped(groupKeys).sum * sf
        }

        val cb = getConfBounds(alpha, aggValue, estimates, n, ns)
        val error = ((cb._1 - cb._2) / 2) / ((cb._1 + cb._2) / 2) * 100

        errorsArray.append(mutable.Map("agg" -> agg,
          "group" -> groupKeys.mkString(","),
          "error" -> BigDecimal(error).setScale(10, BigDecimal.RoundingMode.HALF_UP).toString,
          "ci_low" -> BigDecimal(cb._2).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString,
          "ci_high" -> BigDecimal(cb._1).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString,
          "est" -> BigDecimal(aggValue).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString))
      })

    }

    errorsArray
  }


  def getConfBounds(alpha: Double, est: Double, estimates: Array[Double],
                    n: Integer, ns: Double): (Double, Double) = {

    val diffs = new Array[Double](estimates.length)

    for (i <- estimates.indices) {
      diffs(i) = estimates(i) - est
    }

    var temp = 0.0

    for (i <- estimates.indices) {
      temp += math.pow(estimates(i) - est, 2.0)
    }

    temp = math.sqrt(temp / estimates.size.toDouble)
    println("Error: " + temp)


    val lowerQ = getQuantile(diffs, alpha)
    val upperQ = getQuantile(diffs, 1 - alpha)
    val lowerBound = est - lowerQ * math.sqrt(ns / n)
    val upperBound = est - upperQ * math.sqrt(ns / n)

    (lowerBound, upperBound)
  }


  def getQuantile(diffs: Array[Double], quantile: Double): Double = {

    val diffs2 = diffs.sorted
    val index = (Math.ceil(quantile / 1.0 * diffs2.length)).toInt
    diffs2(index - 1);
  }

}
