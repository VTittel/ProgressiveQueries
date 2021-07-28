package ProgQueries
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}
/*
Result evaluation based on VerdictDB paper

https://arxiv.org/pdf/1804.00770.pdf
 */
class Evaluator extends Serializable {

  /*
  Function to evaluate (report error metrics) single and multi table queries
   */

  // TODO: Fix scaling
  def evaluatePartialResult(temp: Map[(String, Seq[String]), Map[Int, (Double, Double)]],
                            results: Map[(String, Seq[String]), Double],
                            totalCount: Double, sf: Double): mutable.Map[(String, String), Map[String, String]] = {

    val alpha = 0.05
    val ns = math.pow(totalCount, 0.5)

    // (agg, group) -> (error, ci...
    val result = mutable.Map[(String, String), Map[String, String]]()

    for ((aggGroup, value) <- results){
      var aggValue = value
      if (aggGroup._1.contains("sum") || aggGroup._1.contains("count")){
        aggValue = value / sf
      }

      val estimates: ListBuffer[Double] = ListBuffer()

      for ((k,v) <- temp(aggGroup)){
        if (k > 0){
          val subsampSf = 1.0
          val scaledAgg = if (aggGroup._1.contains("sum") || aggGroup._1.contains("count")) v._2 * subsampSf else v._2
          estimates.append(scaledAgg)
        }
      }

      var cb = (0.0, 0.0)
      var error = 1.0

      val scaledAgg = if (aggGroup._1.contains("sum") || aggGroup._1.contains("count")) aggValue * sf else aggValue

      if (estimates.nonEmpty){
        cb = getConfBounds(alpha, aggValue, estimates, totalCount, ns)
        error = ((cb._1 - cb._2) / 2) / ((cb._1 + cb._2) / 2) * 100
      }

      error = if (aggGroup._1.contains("avg")) error / 100.0 else error

      result += ((aggGroup._1, aggGroup._2.mkString(",")) ->
        mutable.Map("error" -> BigDecimal(error).setScale(10, BigDecimal.RoundingMode.HALF_UP).toString,
          "ci_low" -> BigDecimal(cb._2).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString,
          "ci_high" -> BigDecimal(cb._1).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString,
          "est" -> BigDecimal(scaledAgg).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString,
          "count" -> totalCount.toString))
    }

    result

  }


  def getConfBounds(alpha: Double, est: Double, estimates: ListBuffer[Double],
                      n: Double, ns: Double): (Double, Double) = {

    val diffs = new Array[Double](estimates.length)

    for (i <- estimates.indices) {
      diffs(i) = estimates(i) - est
    }

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
