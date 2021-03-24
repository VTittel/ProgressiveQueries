package ProgQueries
import ProgQueries.entryPoint.{excludedFiles, getListOfFiles, getTables}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import java.io.File
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

class SamplingPlanGenerator {

  /*
  Calculate sampling schemes for each table - unused for now
  */
  def pickSamplers(joinInputs: ArrayBuffer[(String, String, String)]): mutable.Map[String, (String, String)] = {
    /*
   	 * Assign a sampling strategy to each table in the query.
   	 * There are 3 samplers at the moment:
   	 * 1. univ - universe
   	 * 2. unif - uniform
   	 * 3. strat - stratified
     */
    val numTables = joinInputs.length + 1
    // (table_name, sampler_type, sample_key)
    var samplers: mutable.Map[String, (String, String)] = mutable.Map()
    // If only 2 tables, use universe sampling on both
    if (numTables == 2) {
      samplers += (joinInputs(0)._1 -> ("univ", joinInputs(0)._3))
      samplers += (joinInputs(0)._2 -> ("univ", joinInputs(0)._3))
    } // If 3 tables, use universe on first 2, and uniform on second
    else if (numTables == 3) {
      samplers += (joinInputs(0)._1 -> ("univ", joinInputs(0)._3))
      samplers += (joinInputs(0)._2 -> ("univ", joinInputs(0)._3))
      samplers += (joinInputs(1)._2 -> ("unif", joinInputs(1)._3))
    } // If 4 or more tables, look if a join sequence contains a subsequence of the same join keys
    else {
      var counts = scala.collection.mutable.Buffer[(String, Integer, Integer)]()
      var prevKey = ""
      var index = -1
      var currPos = 0

      // Calculate subsequence lengths
      for (ji <- joinInputs) {
        val currKey = ji._3

        // If next join key is the same, update its count
        if (currKey == prevKey) {
          //counts(index) = (counts(index)._1, counts(index)._2 + 1, counts(index)._3)
          counts(index) = counts(index).copy(_2 = counts(index)._2 + 1)
          currPos = currPos + 1

        } // Otherwise, add new join key and initialise its count
        else {
          index = index + 1
          counts.append((ji._3, 1, currPos))
          currPos = currPos + 1

        }
        prevKey = currKey
      }

      // Assign samplers
      breakable {
        for (tuple <- counts) {
          val maxElem = counts.maxBy(_._2)
          val maxElemIndex = counts.indexOf(maxElem)
          counts(maxElemIndex) = (counts(maxElemIndex)._1, -1, counts(maxElemIndex)._3)
          // Max element key
          val max_elem_key = maxElem._1
          // Key count
          val subseq_key_count = maxElem._2
          // Where we are in the join chain
          val pos = maxElem._3
          var sampler = ""

          if (subseq_key_count >= 2)
            sampler = "univ"
          else
            sampler = "unif"

          // Store (table, sampler, join_key)
          for (i <- 0 to subseq_key_count) {
            if (((pos + i) == joinInputs.length - 1 || i == subseq_key_count - 1) && !samplers.contains(joinInputs(pos + i)._1)) {
              samplers += (joinInputs(pos + i)._2 -> (sampler, max_elem_key))
            }

            if (!samplers.contains(joinInputs(pos + i)._1))
              samplers += (joinInputs(pos + i)._1 -> (sampler, max_elem_key))
          }

          // update neighbors
          if (maxElemIndex == 0) {
            if (counts(maxElemIndex + 1)._2 != -1)
              counts(maxElemIndex + 1) = counts(maxElemIndex + 1).copy(_2 = math.max(counts(maxElemIndex + 1)._2 - 1, 0))
          } else if (maxElemIndex == counts.size - 1) {
            if (counts(maxElemIndex - 1)._2 != -1)
              counts(maxElemIndex - 1) = counts(maxElemIndex - 1).copy(_2 = math.max(counts(maxElemIndex - 1)._2 - 1, 0))
          } else {
            if (counts(maxElemIndex - 1)._2 != -1)
              counts(maxElemIndex - 1) = counts(maxElemIndex - 1).copy(_2 = math.max(counts(maxElemIndex - 1)._2 - 1, 0))

            if (counts(maxElemIndex + 1)._2 != -1)
              counts(maxElemIndex + 1) = counts(maxElemIndex + 1).copy(_2 = math.max(counts(maxElemIndex + 1)._2 - 1, 0))
          }

        }
      }
    }

    return samplers
  }


  def makeUnivSamples(spark: SparkSession, sampleChoices: mutable.Map[String, (String, String)], p0: Integer,
                      p1: Integer, path: String) {

    val samplers = new Samplers
    val warehouse = "sample_warehouse/"

    for((k,v) <- sampleChoices){
      if (v._1 == "univ") {
        val tableDir = path + k + ".parquet"
        val table = spark.read.parquet(tableDir)
        var tableUniv = samplers.universeSampler(table, k.take(1) + "_" + v._2)
        tableUniv = samplers.rangeSampler(tableUniv, k.take(1) + "_" + v._2 + "_hash", p0, p1)
        val sampleName = warehouse + k + "_" + v._2
        tableUniv.write.partitionBy("univ_partition").parquet(sampleName)
      }
    }
  }

}
