package ProgQueries
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.countDistinct

class MathFunctions {
}

object MathFunctions {

  def mean(list:List[Double]):String = {
    if(list.isEmpty)
      "0.0"
    else
      (list.sum/list.size).toString
  }


  def stdError(list:List[Double]):String = {
    val N = list.size
    var numerator = 0.0
    val meanVal = mean(list).toDouble

    for (hash <- list) {
      numerator += math.pow(hash-meanVal, 2)
    }

    (math.sqrt(numerator / N) / math.sqrt(N.toDouble)).toString
  }


  def countUnique(table: DataFrame, attr: String): String = {

    val count = table.agg(countDistinct(attr).alias("count")).collect().head.getAs("count").toString
    count
  }

}
