package PartitionLogic

import FileHandlers.StatisticsBuilder
import ProgQueries.entryPoint.{excludedFiles, getListOfFiles2, sample}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map}
import scala.util.hashing.MurmurHash3

class PartitionPickerSingleTable(spark: SparkSession, dir: String, statBuild: StatisticsBuilder) {

  val statBuilder = statBuild
  val dataDir = dir
  val statDir = "Statistics_" + dir.slice(dir.indexOf("sf"), dir.length)

  // For each partition, evaluate selectivity
  // TODO: store column type
  def estimateSelectivity(pName: String, exprs: List[List[(String, String, String, String)]], innerConnector: String,
                          outerConnector: String, predCols: List[String], partSize: Long): Double ={

    val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val histograms = mutable.Map[String, ListBuffer[(Long, Long, Long)]]()
    // get all relevant histograms
    for (attr <- predCols)
      histograms += (attr -> statBuilder.getHistogram(statDir + "/" + pName + "/" + attr + "/histogram.txt"))

    var selectivity = if(outerConnector== "and") 1.0 else 0.0
    val depth = math.floor(partSize / 100.0).toLong

    for (clause <- exprs){
      // TODO: fix last index
      var prevSel = if(innerConnector== "and") 1.0 else 0.0
      for (c <- clause){

        val attr = c._1
        val op = c._2
        val colType = c._4
        var const = 0.0
        if (colType == "DateType"){
          const = dateFormat.parse(c._3).getTime
        } else if (colType == "StringType"){
          const = MurmurHash3.stringHash(c._3).toLong
        } else {
          const = c._3.toLong
        }

        val colHist = histograms(attr)

        var sumTuples = 0.0
        // Loop thru buckets and see if const is in bucket
        for (bucket <- colHist){
          val low = bucket._1
          val high = bucket._2
          val distinct = bucket._3
          // =
          if (op == "="){
            // if const is in bucket
            if (const >= low && const <= high) {
              val est = depth / distinct.toDouble
              sumTuples += est
            }
          } else if (op == "<"){
            if (high < const) {
              val est = depth
              sumTuples += est
            }
          } else if (op == ">") {
            if (high > const) {
              val est = depth
              sumTuples += est
            }
          }

          val tmpSel = sumTuples / partSize.toDouble

          if (innerConnector == "and")
            prevSel = math.min(tmpSel, prevSel)
          else if (innerConnector == "or") {
            prevSel = math.min(1.0, prevSel + tmpSel)
          }
          else
            prevSel = tmpSel
        }
      }

      if (outerConnector == "and")
        selectivity = math.min(selectivity, prevSel)
      else if (innerConnector == "or") {
        selectivity = math.min(1.0, prevSel + selectivity)
      }
      else
        selectivity = prevSel
    }

    math.min(selectivity, 1.0)
  }

  /*
 Weigh partitions. Take into account selectivity, cardinality of all group by columns. Define outlier score for
 each group by column.
 Selectivity and outlier score stays static. Cardinality score changes.
 Keep weights static for now
 */
  def weighPartitions(partitions: ListBuffer[Partition], groupingCols: List[String]): Map[Partition, Double] = {
    // Calculate outlier score
    // Top k groups
    var topKList: List[String] = List()

    for (partition <- partitions){
      var allHeavyHitters: List[String] = List()
      for (col <- groupingCols){
        val localHeavyHitters = statBuilder.getHeavyHittersAsList(statDir + "/" + partition.pTable + "/" +
          partition.pName + "/" + col + "/heavyhitters.txt").toList

        allHeavyHitters = allHeavyHitters ++ localHeavyHitters
      }
      // replace local heavy hitters with global ones
      partition.setHeavyHitters(allHeavyHitters)
      topKList = topKList ++ allHeavyHitters
    }

    // Group partitions by their heavy hitter list
    val topKSet = topKList.toSet
    val topKGroups: Map[Set[String], ListBuffer[Partition]] = Map()

    for (partition <- partitions){
      val partHeavyHitters = partition.getHeavyHitters().toSet
      val globalHHinPart = partHeavyHitters.intersect(topKSet)

      if (topKGroups.contains(globalHHinPart)){
        topKGroups(globalHHinPart).append(partition)
      } else {
        topKGroups += (globalHHinPart -> ListBuffer(partition))
      }
    }

    var maxGroups = 0
    var minGroups = Integer.MAX_VALUE
    for ((k,v) <- topKGroups){
      val groupCount = v.size

      if (groupCount > maxGroups)
        maxGroups = groupCount

      if (groupCount < minGroups)
        minGroups = groupCount
    }

    for ((k,v) <- topKGroups){
      val outlierScore = 1.0 - (v.size - minGroups).toDouble / math.max(1.0,(maxGroups - minGroups).toDouble)

      for (partition <- v)
        partition.setOutlierScore(outlierScore)
    }

    val weightedPartitions: Map[Partition, Double] = Map()
    // selectivity + all cardinalities + outlier score
    val multiplier = 0.6 / (groupingCols.size.toDouble)
    val selMultiplier = 0.4

    for (partition <- partitions) {
      var weight = selMultiplier*partition.getSelectivity() + multiplier*partition.getOutlierScore()

      for (gcol <- groupingCols)
        weight += multiplier*partition.getCardinality(gcol)

      weightedPartitions += (partition -> math.max(weight*100.0, 1.0))
    }

    weightedPartitions

  }

  // Algorithm to create partition weights
  def createPartitions(table: String, hasPred: Boolean, exprs: List[List[(String, String, String, String)]], innerConnector: String,
                     outerConnector: String, predCols: List[String], groupingCols: List[String],
                       isRandom: Boolean): Map[Partition, Double] = {

    val filteredPartitions: ListBuffer[Partition] = ListBuffer()

    val partitionNames = getListOfFiles2(dataDir + table, excludedFiles)

    if (isRandom) {
      val weightedPartitions: Map[Partition, Double] = Map()
      for (partition <- partitionNames) {
        val partSize = statBuilder.getStatAsString(statDir + "/" + table + "/" + partition.getName + "/numtuples.txt").toLong
        val pName = partition.getName
        val partitionObject = new Partition(pName, table, partSize)
        weightedPartitions += (partitionObject -> 1.0)
      }
      return weightedPartitions
    }

    for (partition <- partitionNames) {
      val partSize = statBuilder.getStatAsString(statDir + "/" + table + "/" + partition.getName + "/numtuples.txt").toLong
      val selectivity = if (hasPred) estimateSelectivity(table + "/" + partition.getName, exprs, innerConnector, outerConnector,
        predCols, partSize) else 1.0

      if (selectivity > 0.0) {
        val pName = partition.getName
        val partitionObject = new Partition(pName, table, partSize)
        partitionObject.setSelectivity(selectivity)
        // get cardinality and heavy hitters for each grouping col
        for (col <- groupingCols){
          val cardinality = statBuilder.getStatAsString(statDir + "/" + table + "/" + partition.getName + "/" +
            col + "/cardinality.txt").toDouble

          partitionObject.setCardinality(col, cardinality)
        }

        filteredPartitions.append(partitionObject)
      }
    }


    val weightedPartitions = weighPartitions(filteredPartitions, groupingCols)
    weightedPartitions
  }


  def samplePartitions(partitions: Map[Partition, Double], p: Int): ListBuffer[Partition] = {
    val sampledPartitions: ListBuffer[Partition] = ListBuffer()

    for (i <- 0 until p) {
      val samplePartition = sample(partitions)
      sampledPartitions.append(samplePartition)
      // remove partition from further consideration
      partitions -= samplePartition
    }

    sampledPartitions
  }

}
