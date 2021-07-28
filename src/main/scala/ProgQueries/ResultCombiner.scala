package ProgQueries

import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}

class ResultCombiner {

  // (A, G) --> (sid --> (count, val))
  val cache: Map[(String, Seq[String]), Map[Int, (Double, Double)]] = Map()
  var totalTupleCount = 0.0

  def combine(resultDF: DataFrame,
              aggregates: ArrayBuffer[(String, String, String)],
              sf: Double): mutable.Map[(String, String), Map[String, String]] ={

    val subsampPerAgg = mutable.Map[String, Array[Seq[String]]]()

    val aggStrings = aggregates.map(t => t._3)
    var projections: Seq[String] = (resultDF.schema.fieldNames.toSet).filterNot(aggStrings.toSet).map(i => i.toString).toSeq
    if (!projections.contains("sid"))
      projections = projections :+ "sid"

    // Calculate subsamples for each aggregate and store in an array
    // length of row - hack, needed for subsample extraction
    var rowLen = 0
    aggStrings -= "sid"

    for (agg <- aggStrings){
      val subsamples = resultDF.select(agg, projections: _*).rdd.map(row => row.toSeq.map(_.toString))
        .collect()

      rowLen = subsamples.head.length

      subsampPerAgg += (agg -> subsamples)
    }

    // G -> (sid, count)
    val tupleCountPerSid = subsampPerAgg("count_tuples").groupBy(_.tail.dropRight(1)).map{case (k,v) => k ->
      v.map(x => (x.last.toInt, x.head.toDouble)).toMap}

    val tupleCount: Double = tupleCountPerSid(tupleCountPerSid.keys.head).values.sum
    totalTupleCount += tupleCount

    val results: Map[(String, Seq[String]), Double] = Map()

    for (agg <- aggStrings) {
      // (G --> (sid --> val))
      val grouped = subsampPerAgg(agg).groupBy(_.tail.dropRight(1)).map{case (k,v) => k ->
        v.map(x => (x.last.toInt, x.head.toDouble)).toMap}

      for ((group, sidToValue) <- grouped){

        /*
        sid --> (count, val)
        for all groups in the aggregate, create a map that links their value and tuple count to sid
         */
        // If cache for the (agg, group) combination is empty, add it
        if (!cache.contains((agg, group))) {

          val sidToCountValue: Map[Int, (Double, Double)] = Map()

          // for all SIDs loop
          for ((sid, value) <- sidToValue) {
            val sidTupleCount = tupleCountPerSid(group)(sid)

            sidToCountValue += (sid -> (sidTupleCount, value))
          }

          cache += ((agg, group) -> sidToCountValue)

        } else {  // else, combine the values
          val cachedSidToCountValue = cache((agg, group))

          // for all SIDs loop
          for ((sid, value) <- sidToValue) {
            val sidTupleCountNew = tupleCountPerSid(group)(sid)
            val sidTupleCountOld = if (cachedSidToCountValue.contains(sid)) cachedSidToCountValue(sid)._1 else 0.0
            val sidValueOld = if (cachedSidToCountValue.contains(sid)) cachedSidToCountValue(sid)._2 else 0.0
            var newSidAggValue = 0.0

            if (agg.contains("avg")) {
              newSidAggValue = updateAVG(sidValueOld, value, sidTupleCountOld, sidTupleCountNew)
            } else if (agg.contains("sum") || agg.contains("count")){
              newSidAggValue = sidValueOld + value
            }

            // update cache
            cache((agg, group))(sid) = (sidTupleCountNew + sidTupleCountOld, newSidAggValue)
          }
        }

        // Calculate answers
        val sidToCountValue = cache((agg, group))
        // estimate, combination of all values for all sid
        var est = 0.0
        var tupleCountSoFar = 0.0
        for ((sid, countValue) <- sidToCountValue){

          if (agg.contains("avg")) {
            est += countValue._2
            //est = updateAVG(countValue._2, est, tupleCountSoFar, countValue._1)
          } else if (agg.contains("sum") || agg.contains("count")){
            est += countValue._2
          }
          // keep track of how many tuples we have seen so far in the calculation.
          tupleCountSoFar += countValue._1
        }

        val finalEst = if (agg.contains("sum") || agg.contains("count")) est*sf else est / 100.0
        println(agg, group)
        println(finalEst)
        results += ((agg, group) -> finalEst)
      }
    }

    // Feed the processed aggregates to error estimator
    val evaluator = new Evaluator()
    val res = evaluator.evaluatePartialResult(cache, results, totalTupleCount, sf)
    res
  }


  def updateAVG(avgOld: Double, avgNew: Double, nOld: Double, nNew: Double): Double = {
    val updatedAvg = avgNew * (nNew / (nNew + nOld)) + avgOld * (nOld / (nNew + nOld))
    updatedAvg
  }


}
