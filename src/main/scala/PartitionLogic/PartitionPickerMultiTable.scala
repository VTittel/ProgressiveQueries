package PartitionLogic

import FileHandlers.StatisticsBuilder
import ProgQueries.entryPoint.{excludedFiles, getListOfFiles2, sample}
import org.apache.spark.sql.SparkSession

import java.io.File
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}
import scala.util.hashing.MurmurHash3
import scala.util.Random

class PartitionPickerMultiTable(spark: SparkSession, jg: JoinGraphV2, dir: String, statBuild: StatisticsBuilder) {

  val statBuilder = statBuild
  val dataDir = dir
  val statDir = "Statistics_" + dir.slice(dir.indexOf("sf"), dir.length)
  val joinGraph = jg

  // For each partition, evaluate selectivity
  // TODO: store column type
  def estimateSelectivity(pName: String, exprs: List[List[(String, String, String, String)]], innerConnector: String,
                          outerConnector: String, predCols: List[String], partSize: Long): Double ={

    val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val histograms = mutable.Map[String, ListBuffer[(Long, Long, Long)]]()
    // get all relevant histograms
    for (attr <- predCols) {
      histograms += (attr -> statBuilder.getHistogram(statDir + "/" + pName + "/" + attr + "/histogram.txt"))
    }

    var selectivity = if(outerConnector== "and") 1.0 else 0.0
    val depth = math.floor(partSize / 10.0).toLong
    var prevSel = 0.0
    for (clause <- exprs){
      for (c <- clause){

        val attr = c._1
        val op = c._2
        val colType = c._4
        var const = 0L
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
            if (high > const) {
              val est = depth
              sumTuples += est
            }
          } else if (op == ">") {
            if (low < const) {
              val est = depth
              sumTuples += est
            }
          }

          val tmpSel = sumTuples / partSize.toDouble
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
  Estimate join sizes between partitions
   */
  def estimateJoinSizes(filteredPartitionsPerTable: mutable.Map[String, List[File]],
                        joinInputs: ArrayBuffer[(String, String, String, String)]): Any = {


    for (((k,v), index) <- filteredPartitionsPerTable.zipWithIndex){
      for (path <- v){
        val nType = if (index == 0) "start" else if (index == filteredPartitionsPerTable.size - 1) "end" else "interm"
        val vertex = new joinGraph.Node(path.getName, nType, k)
        joinGraph.addVertex(vertex)
      }

    }

    for (ji <- joinInputs)
      joinGraph.addEdges(ji._1, ji._2)


    val paths: ListBuffer[ListBuffer[Set[joinGraph.Node]]] = joinGraph.calculatePaths()
 //   val paths = Random.shuffle(pathsTemp).take((pathsTemp.size.toDouble / 10.0).toInt)
    val eligiblePaths: ListBuffer[ListBuffer[Set[joinGraph.Node]]] = ListBuffer()
    // TODO: change to calculate cms inner product only once for each unique combination
    // cache join size
    val estCache: mutable.Map[String, Long] = mutable.Map()

    for (path <- paths){
      var size = 1

      for (i <- 0 to path.length - 2){
        val leftNodeName = path(i).head.getName()
        val leftNodeKey = joinInputs(i)._3.replace("'", "")
        val leftNodeTable = path(i).head.getTableName()
        val rightNodeName = path(i+1).head.getName()
        val rightNodeKey = joinInputs(i)._4.replace("'", "")
        val rightNodeTable = path(i+1).head.getTableName()

        val key = leftNodeTable + leftNodeName + leftNodeKey + rightNodeTable + rightNodeName + rightNodeKey
        var joinSize = 0L

        if (estCache.contains(key))
          joinSize = estCache(key)
        else {
          val cmsLeft = statBuilder.getCMS(leftNodeTable, leftNodeName, leftNodeKey)
          val cmsRight = statBuilder.getCMS(rightNodeTable, rightNodeName, rightNodeKey)
          joinSize = cmsLeft.innerProduct(cmsRight).estimate
          estCache += (key -> joinSize)
        }
        val partSizeLeft = statBuilder.getStatAsString(statDir + "/" + leftNodeTable + "/" + leftNodeName + "/" +
          "numtuples.txt").toLong
        val partSizeRight = statBuilder.getStatAsString(statDir + "/" + rightNodeTable + "/" + rightNodeName + "/" +
          "numtuples.txt").toLong
        val EPS = 0.000000001

       // if (joinSize*10000.0 < partSizeLeft * partSizeRight * EPS)
        if (joinSize == 0)
          size = 0
      }

      if (size == 1){
        eligiblePaths.append(path)
      }

    }

    eligiblePaths
  }


    /*
   Weigh partitions. Take into account selectivity, cardinality of all group by columns. Define outlier score for
   each group by column.
   Selectivity and outlier score stays static. Cardinality score changes.
   Keep weights static for now
   */
  def weighPartitions(joinPaths: ListBuffer[JoinPath[joinGraph.Node]], groupingCols: Map[String, List[String]]):
                      Map[JoinPath[joinGraph.Node], Double] = {
    // Calculate outlier score
    // Top k groups
    var topKList: List[String] = List()

    for (joinPath <- joinPaths){
      // Gather all local heavy hitters (all cols combined)
      var allHeavyHitters: List[String] = List()

      for ((table, cols) <- groupingCols){
        for (col <- cols){
          val localHeavyHitters = joinPath.getHeavyHitters(table, col)
          allHeavyHitters = allHeavyHitters ++ localHeavyHitters
        }
      }

      joinPath.setAllLocalHeavyHitters(allHeavyHitters)
      topKList = topKList ++ allHeavyHitters
    }

    // Group partitions by their heavy hitter list
    val topKSet = topKList.toSet
    val topKGroups: Map[Set[String], ListBuffer[JoinPath[joinGraph.Node]]] = Map()

    for (joinPath <- joinPaths){
      val joinPathHH = joinPath.getAllLocalHeavyHitters().toSet
      val globalHHinPath = joinPathHH.intersect(topKSet)

      if (topKGroups.contains(globalHHinPath)){
        topKGroups(globalHHinPath).append(joinPath)
      } else {
        topKGroups += (globalHHinPath -> ListBuffer(joinPath))
      }
    }

    // Normalize outlier score
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
      for (joinPath <- v)
       joinPath.setOutlierScore(outlierScore)
    }

    // Normalize cardinality and selectivity
    val maxCard: Map[String, Double] = Map()
    val minCard: Map[String, Double] = Map()
    var maxSel = 0.0
    var minSel = 1.0

    for (joinPath <- joinPaths) {
      // cardinality
      for ((table, cols) <- groupingCols) {
        for (col <- cols) {
          val cardinality = joinPath.getCardinality(col)
          if (maxCard.contains(col)) {
            if (maxCard(col) < cardinality)
              maxCard(col) = cardinality
          } else
            maxCard += (col -> cardinality)

          if (minCard.contains(col)) {
            if (minCard(col) > cardinality)
              minCard(col) = cardinality
          } else
            minCard += (col -> cardinality)
        }
      }
      // selectivity
      val selectivity = joinPath.getSelectivity()

      if (maxSel < selectivity)
        maxSel = selectivity

      if (minSel > selectivity)
        minSel = selectivity

    }


    for (joinPath <- joinPaths) {
      // cardinality
      for ((table, cols) <- groupingCols) {
        for (col <- cols) {
          if (!(table == "region" || table == "nation" || table == "supplier") && !(maxCard(col) == minCard(col))){
            val normCardinality = (joinPath.getCardinality(col) - minCard(col)) / (maxCard(col) - minCard(col))
            joinPath.setCardinality(col, normCardinality)
          }
        }
      }
      //selectivity
      if (!(maxSel == minSel)) {
        val normSel = (joinPath.getSelectivity() - minSel) / (maxSel - minSel)
        joinPath.setSelectivity(normSel)
      }
    }


    val weightedJoinPaths: Map[JoinPath[joinGraph.Node], Double] = Map()

    // selectivity + all cardinalities + outlier score
    val cMultiplier = 0.35 / (groupingCols.foldLeft(0)(_+_._2.size).toDouble)
    val selMultiplier = 0.1
    val oMultiplier = 0.55

    for (joinPath <- joinPaths) {
      var weight = selMultiplier*joinPath.getSelectivity() + oMultiplier*joinPath.getOutlierScore()

      for ((table, cols) <- groupingCols) {
        for (col <- cols) {
          weight += cMultiplier * joinPath.getCardinality(col)
        }
      }

      weightedJoinPaths += (joinPath -> math.max(weight*100.0, 1.0))
    }

    weightedJoinPaths

  }


  def createPartitions(tableNames: Seq[String], exprs: Map[String, List[List[(String, String, String, String)]]],
                       innerConnector: String, outerConnector: String, predTableCols: Map[String, List[String]],
                       joinInputs: ArrayBuffer[(String, String, String, String)],
                       groupingCols: Map[String, List[String]], isRandom: Boolean): Map[JoinPath[joinGraph.Node], Double] = {

    if (isRandom){
      for ((table, index) <- tableNames.zipWithIndex){
        val partitions = getListOfFiles2(dataDir + table, excludedFiles)
        for (part <- partitions){
          val nType = if (index == 0) "start" else if (index == tableNames.size - 1) "end" else "interm"
          val vertex = new joinGraph.Node(part.getName, nType, table)
          joinGraph.addVertex(vertex)
        }

      }

      for (ji <- joinInputs)
        joinGraph.addEdges(ji._1, ji._2)

      val paths: ListBuffer[ListBuffer[Set[joinGraph.Node]]] = joinGraph.calculatePaths()
      val weightedPathObjects: Map[JoinPath[joinGraph.Node], Double] = Map()

      for (path <- paths){
        val pathObject = new JoinPath(path, joinGraph)
        weightedPathObjects += (pathObject -> 1.0)
      }

      return weightedPathObjects
    }


    val filteredPartitionsPerTable: mutable.LinkedHashMap[String, List[File]] = mutable.LinkedHashMap()

    // (table, col) -> sel
    val selectivityStore: mutable.Map[(String, String), Double] = mutable.Map()

    // 1. Filter our all partitions with selectivity = 0.0
    for (table <- tableNames){
      val partitions = getListOfFiles2(dataDir + table, excludedFiles)

      if (!predTableCols.contains(table)){
        filteredPartitionsPerTable += (table -> partitions)
      } else {
        val predCols = predTableCols(table)
        val filteredPartitions: ListBuffer[File] = ListBuffer()
        for (partition <- partitions) {
          val partSize = statBuilder.getStatAsString(statDir + "/" + table + "/" + partition.getName + "/" +
            "numtuples.txt").toLong

          var selectivity = 0.0
          if (table == "region" || table == "nation" || table == "supplier")
            selectivity = 0.01
          else
            selectivity = estimateSelectivity(table + "/" + partition.getName, exprs(table), innerConnector,
            outerConnector, predCols, partSize)

          if (selectivity > 0.0) {
            selectivityStore += ((table, partition.getName) -> selectivity)
            filteredPartitions.append(partition)
          }
        }
        filteredPartitionsPerTable += (table -> filteredPartitions.toList)
      }

    }

    // 2. Filter out partitions that dont join
    val eligiblePaths = estimateJoinSizes(filteredPartitionsPerTable, joinInputs)
      .asInstanceOf[ListBuffer[ListBuffer[Set[joinGraph.Node]]]]



    // 3. Calculate stats for each path
    val pathObjects: ListBuffer[JoinPath[joinGraph.Node]] = ListBuffer()
    for (path <- eligiblePaths) {
      var selectivity = if(outerConnector== "and") 1.0 else 0.0
      val pathObject = new JoinPath(path, joinGraph)
      for (node <- path) {
        // add partition sizes
        val partSize = statBuilder.getStatAsString(statDir + "/" + node.head.getTableName() + "/" + node.head.getName()
          + "/" + "numtuples.txt").toLong
        pathObject.setTupleCount(node.head.getTableName() + "/" + node.head.getName(), partSize)
        // Find where clause node
        for ((k,v) <- selectivityStore) {
          if (node.head.getTableName() == k._1 && node.head.getName() == k._2) {
            if (outerConnector == "and")
              selectivity = math.min(selectivity, v)
            else
              selectivity = math.max(1.0, selectivity + v)
          }
        }

        // Find group by nodes
        for ((k,v) <- groupingCols){
          for (group <- v){
            if (node.head.getTableName() == k){
              val cardinality = statBuilder.getStatAsString(statDir + "/" + node.head.getTableName() + "/"
                + node.head.getName() + "/" + group + "/cardinality.txt").toDouble
              val heavyHitters = statBuilder.getHeavyHittersAsList(statDir + "/" + node.head.getTableName() + "/"
                + node.head.getName() + "/" + group + "/heavyhitters.txt").toList

              pathObject.setCardinality(group, cardinality)
              pathObject.setHeavyHitters(k, group,heavyHitters)
            }
          }
        }
        pathObject.setSelectivity(selectivity)
      }
      pathObjects.append(pathObject)
    }

    /*
    for (path <- pathObjects){
      println(path.getSelectivity())
      for ((table, cols) <- groupingCols){
        for (col <- cols){
          println(path.getCardinality(col))
          println(path.getHeavyHitters(table, col))
        }
      }
      println("**************************************************************")
    }

     */



    val weightedPartitions = weighPartitions(pathObjects, groupingCols)
    weightedPartitions

  }

  /*
  Function to sample a list of paths within a partition budget p. Keep sampling paths until total number of
  partitions is p, or slightly more.
   */
  def samplePaths(weightedJoinPaths: Map[JoinPath[joinGraph.Node], Double], p: Int): ListBuffer[JoinPath[joinGraph.Node]] ={

    // total number of partitions sampled so far
    var n = 0
    val sampledPaths: ListBuffer[JoinPath[joinGraph.Node]] = ListBuffer()
    val partitionsSoFar: ListBuffer[String] = ListBuffer()

    while( n < p){
      val path = sample(weightedJoinPaths)
      sampledPaths += path
      weightedJoinPaths -= path

      for (node <- path.getNodes()){
        val partition = node.head.getTableName() + "/" + node.head.getName()
        if (!partitionsSoFar.contains(partition)) {
//          partitionCounts(node.head.getTableName()) += 1
          partitionsSoFar += partition
        }
      }

      n = partitionsSoFar.size
    }

    sampledPaths
  }
}
