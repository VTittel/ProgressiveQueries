package ProgQueries
import ProgQueries.entryPoint.{excludedFiles, getListOfFiles, getListOfFiles2}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{ListBuffer, Map}
import org.apache.spark.sql.functions.countDistinct
import ProgQueries.StatisticsBuilder
import ProgQueries.MathFunctions._
import Query.QueryParser

import scala.util.Random
import java.io.{File, ObjectOutputStream}
import scala.collection.mutable
import scala.io.Source
import java.io._


class Experimental {

  // TODO:
  //  1. Account for where clause
  //  2. Account for different partition sizes

  def doStuff(spark: SparkSession): Unit = {
    val query = Query.BenchQueries.QueryFiveTemp
    val samplers = new Samplers
    val Stats = new StatisticsBuilder(spark)

    //p_container = 40, p_brand = 25, p_type = 150
    /*
    var partSample = spark.read.parquet("data_parquet/part.parquet")
    partSample = samplers.universeSamplerV2(partSample, "p_partkey", 0.01)
    var partsuppSample = spark.read.parquet("data_parquet/partsupp.parquet")
    partsuppSample = samplers.universeSamplerV2(partsuppSample, "ps_partkey", 0.01)
    var lineitemSample = spark.read.parquet("data_parquet/lineitem.parquet")
    lineitemSample = samplers.universeSamplerV2(lineitemSample, "l_partkey", 0.01)

    partSample.createOrReplaceTempView("part")
    partsuppSample.createOrReplaceTempView("partsupp")
    lineitemSample.createOrReplaceTempView("lineitem")

    val df = spark.sql(query)
    println(df.count())

     */

    /*
    Stats.calculateMeasure("part.parquet", "p_partkey", "mean")
    Stats.calculateMeasure("part.parquet", "p_partkey", "stdError")
    Stats.calculateMeasure("part.parquet", "p_container", "countUnique")
    Stats.calculateMeasure("partsupp.parquet", "ps_partkey", "mean")
    Stats.calculateMeasure("partsupp.parquet", "ps_partkey", "stdError")
    Stats.calculateMeasure("lineitem.parquet", "l_partkey", "mean")
    Stats.calculateMeasure("lineitem.parquet", "l_partkey", "stdError")

     */


    val partPartitions = getListOfFiles(new File("Statistics/part.parquet"), excludedFiles)
    val partsuppPartitions = getListOfFiles(new File("Statistics/partsupp.parquet"), excludedFiles)
    val lineitemPartitions = getListOfFiles(new File("Statistics/lineitem.parquet"), excludedFiles)


    val joinGraph = new JoinGraphV2()

    for (pp <- partPartitions) {
      val vertex = new joinGraph.Node(pp.toString, "start", "part")
      joinGraph.addVertex(vertex)
    }

    for (psp <- partsuppPartitions) {
      val vertex = new joinGraph.Node(psp.toString, "interm", "partsupp")
      joinGraph.addVertex(vertex)
    }

    for (lp <- lineitemPartitions) {
      val vertex = new joinGraph.Node(lp.toString, "end", "lineitem")
      joinGraph.addVertex(vertex)
    }

    joinGraph.addEdges("part", "partsupp")
    joinGraph.addEdges("partsupp", "lineitem")

    val paths: ListBuffer[ListBuffer[Set[joinGraph.Node]]] = joinGraph.calculatePaths()

    val ZScores: Map[ListBuffer[Set[joinGraph.Node]], Double] = Map()

    for (path <- paths){
      val partStatsPartkey = Stats.getStatsAsMap(path(0).head.getName() + "/p_partkey/statistics.txt")
      val partsuppStatsPartkey = Stats.getStatsAsMap(path(1).head.getName() + "/ps_partkey/statistics.txt")
      val lineitemStatsPartkey = Stats.getStatsAsMap(path(2).head.getName() + "/l_partkey/statistics.txt")

      if (path(2).head.getName() == "Statistics/lineitem.parquet/part-00058-53a0d00a-462d-4f93-9292-5060a899e76b-c000.snappy.parquet") {
        val k = 1 / 0.023222987

        val meanDiff1 = math.abs(partStatsPartkey("mean").toDouble - partsuppStatsPartkey("mean").toDouble)
        val meanDiff2 = math.abs(partsuppStatsPartkey("mean").toDouble - lineitemStatsPartkey("mean").toDouble * k)

        var ZScore = meanDiff1 / math.sqrt(partStatsPartkey("stdError").toDouble + partsuppStatsPartkey("stdError").toDouble)
        ZScore += meanDiff2 / math.sqrt(partsuppStatsPartkey("stdError").toDouble + lineitemStatsPartkey("stdError").toDouble * k)
        ZScores += (path -> ZScore)
      } else {
        val meanDiff1 = math.abs(partStatsPartkey("mean").toDouble - partsuppStatsPartkey("mean").toDouble)
        val meanDiff2 = math.abs(partsuppStatsPartkey("mean").toDouble - lineitemStatsPartkey("mean").toDouble)

        var ZScore = meanDiff1 / math.sqrt(partStatsPartkey("stdError").toDouble + partsuppStatsPartkey("stdError").toDouble)
        ZScore += meanDiff2 / math.sqrt(partsuppStatsPartkey("stdError").toDouble + lineitemStatsPartkey("stdError").toDouble)
        ZScores += (path -> ZScore)
      }
    }

    val minGroup = 40.0
    val maxGroup = 40.0
    var bestPath: ListBuffer[Set[joinGraph.Node]] = ListBuffer(Set(Option.empty[joinGraph.Node].orNull))
    var maxWeight = 0.0
    var maxZ = 1.7412023510580476E7
    var minZ = 458762.2201912994


    val sampleZScores = Random.shuffle(ZScores).take(20)

    for ((k,v) <- sampleZScores) {
      val numGroups = Stats.getStatsAsMap(k(0).head.getName() + "/p_container/statistics.txt")("countUnique").toDouble
      //val normNumGroups = (numGroups - minGroup) / (maxGroup - minGroup)
      val normNumGroups = 1.0

      val normZ = (v - minZ) / (maxZ - minZ)

      val weight = if (normNumGroups == 0.0) 0.5*(1-normZ) else 0.5*(1-normZ) + 0.5*normNumGroups

      if (weight > maxWeight) {
        maxWeight = weight
        bestPath = k
      }

      /*
      for (node <- k)
        println(node.head.getName())

       */


      var partSample = spark.read.parquet(k(0).head.getName()
        .replace("Statistics", "data_parquet"))
      partSample = samplers.universeSamplerV2(partSample, "p_partkey", 0.04)
      var partsuppSample = spark.read.parquet(k(1).head.getName()
        .replace("Statistics", "data_parquet"))
      partsuppSample = samplers.universeSamplerV2(partsuppSample, "ps_partkey", 0.09)
      var lineitemSample = spark.read.parquet(k(2).head.getName()
        .replace("Statistics", "data_parquet"))
      lineitemSample = samplers.universeSamplerV2(lineitemSample, "l_partkey", 0.59)

      partSample.createOrReplaceTempView("part")
      partsuppSample.createOrReplaceTempView("partsupp")
      lineitemSample.createOrReplaceTempView("lineitem")


      val df = spark.sql(query)
      println(df.count(), normZ)


      if (v < minZ)
        minZ = v

      if (v > maxZ)
        maxZ = v

    }

    //println(maxZ)
    //println(minZ)

  }


  def doOtherStuff(spark: SparkSession): Unit = {
    val query = Query.BenchQueries.QuerySevenAvg
    val samplers = new Samplers
    val Stats = new StatisticsBuilder(spark)

    val qp = new QueryParser
    val joinInputs = qp.parseQueryJoin(spark, query)

    val fullResMap = Map[List[String], String]()
    val full = spark.read.option("header", true).csv("spark-warehouse/q7avg_exactsf10_skew.csv")
    full.collect().foreach(row => fullResMap += List(row.getAs("o_orderdate").toString)
      -> row.getAs[String]("avg_revenue"))

    /*
    Stats.createCMS(spark, "customer.parquet", "c_custkey")
    Stats.createCMS(spark, "order.parquet", "o_custkey")
    Stats.createCMS(spark, "order.parquet", "o_orderkey")
    Stats.createCMS(spark, "lineitem.parquet", "l_orderkey")
    Stats.calculateMeasure("customer.parquet", "c_custkey", "mean")
    Stats.calculateMeasure("customer.parquet", "c_custkey", "stdError")
    Stats.calculateMeasure("order.parquet", "o_custkey", "mean")
    Stats.calculateMeasure("order.parquet", "o_custkey", "stdError")
    Stats.calculateMeasure("order.parquet", "o_orderkey", "mean")
    Stats.calculateMeasure("order.parquet", "o_orderkey", "stdError")

    Stats.calculateMeasure("lineitem.parquet", "l_orderkey", "mean")
    Stats.calculateMeasure("lineitem.parquet", "l_orderkey", "stdError")
     */
    //Stats.calculateMeasure("order.parquet", "o_orderdate", "countUnique")

    // Idea: Store all cms objects in one mega object, for faster loading
    Stats.loadCMS()

    val custPartitions = getListOfFiles(new File("Statistics/customer.parquet"), excludedFiles)
    val orderPartitions = getListOfFiles(new File("Statistics/order.parquet"), excludedFiles)
    val lineitemPartitions = getListOfFiles(new File("Statistics/lineitem.parquet"), excludedFiles)


    val joinGraph = new JoinGraphV2()

    for (cp <- custPartitions) {
      val vertex = new joinGraph.Node(cp.getName, "start", "customer.parquet")
      joinGraph.addVertex(vertex)
    }

    for (op <- orderPartitions) {
      val vertex = new joinGraph.Node(op.getName, "interm", "order.parquet")
      joinGraph.addVertex(vertex)
    }

    for (lp <- lineitemPartitions) {
      val vertex = new joinGraph.Node(lp.getName, "end", "lineitem.parquet")
      joinGraph.addVertex(vertex)
    }

    joinGraph.addEdges("customer.parquet", "order.parquet")
    joinGraph.addEdges("order.parquet", "lineitem.parquet")

    val paths: ListBuffer[ListBuffer[Set[joinGraph.Node]]] = joinGraph.calculatePaths()

    val joinSizes: Map[ListBuffer[Set[joinGraph.Node]], Double] = Map()
    val groupByUniqueCounts: Map[ListBuffer[Set[joinGraph.Node]], Double] = Map()
    // TODO: change to calculate cms inner product only once for each unique combination
    // IDEA: maybe use some decision tree?

    for (path <- paths){
      var totalSize = 1.0

      for (i <- 0 to path.length - 2){
        val leftNodeName = path(i).head.getName()
        val leftNodeKey = joinInputs(i)._3
        val leftNodeTable = path(i).head.getTableName()
        val rightNodeName = path(i+1).head.getName()
        val rightNodeKey = joinInputs(i)._4
        val rightNodeTable = path(i+1).head.getTableName()

        val cmsLeft = Stats.getCMS(leftNodeTable, leftNodeName, leftNodeKey)
        val cmsRight = Stats.getCMS(rightNodeTable, rightNodeName, rightNodeKey)
        val joinSize = cmsLeft.innerProduct(cmsRight).estimate.toDouble

        if (joinSize > 0)
          totalSize *= joinSize
      }

      joinSizes+=(path -> totalSize)

      val orderStatsOrderdate = Stats.getStatsAsMap("Statistics/" +  path(1).head.getTableName + "/" +
        path(1).head.getName() + "/o_orderdate/statistics.txt")
      groupByUniqueCounts +=(path -> orderStatsOrderdate("countUnique").toDouble)
    }

    val sortedJoinSizes = joinSizes.toList.sortWith(_._2 > _._2)
    val maxJoinSize = sortedJoinSizes.head._2
    val minJoinSize = sortedJoinSizes.last._2
    val sortedGroupByUniqueCounts = groupByUniqueCounts.toList.sortWith(_._2 > _._2)
    val maxGroupCount = sortedGroupByUniqueCounts.head._2
    val minGroupCount = sortedGroupByUniqueCounts.last._2

    val pathWeights: Map[ListBuffer[Set[joinGraph.Node]], Double] = Map()

    for (path <- paths) {
      val numGroups = groupByUniqueCounts(path)
      val joinSize = joinSizes(path)

      var weight = 0.0
      if (numGroups > 0.0 && joinSize > 0.0){
        val normJoinSize = (joinSize - minJoinSize) / (maxJoinSize - minJoinSize)
        val normNumGroups = (numGroups - minGroupCount) / (maxGroupCount - minGroupCount)
        weight = 0.5*normJoinSize + 0.5*normNumGroups
      }

      pathWeights += (path -> weight)
    }

    val sortedPathWeights = pathWeights.toList.sortWith(_._2 > _._2)
//    var topK = sortedPathWeights.take(10)

    var topK = Random.shuffle(sortedPathWeights)
    topK = topK.take(10)

    var customerCache = spark.emptyDataFrame
    var orderCache = spark.emptyDataFrame
    var lineitemCache = spark.emptyDataFrame
    var cachedCustomerPartitions: ListBuffer[String] = ListBuffer()
    var cachedOrderPartitions: ListBuffer[String] = ListBuffer()
    var cachedLineitemPartitions: ListBuffer[String] = ListBuffer()

    // TODO: If universe partition was already sampled, sample from different space

    for (path <- topK) {

     // joinGraph.printPath(path._1)


      var customerSample = spark.read.parquet("data_parquet/" + path._1(0).head.getTableName() + "/" +
        path._1(0).head.getName())
      customerSample = samplers.universeSamplerV2(customerSample, "c_custkey", 0.04)
      var orderSample = spark.read.parquet("data_parquet/" + path._1(1).head.getTableName() + "/" +
        path._1(1).head.getName())
      orderSample = samplers.universeSamplerV2(orderSample, "o_custkey", 0.14)
      val lineitemSample = spark.read.parquet("data_parquet/" + path._1(2).head.getTableName() + "/" +
        path._1(2).head.getName()).sample(0.59)

      if (customerCache.isEmpty)
        customerCache = customerSample
      else if (!cachedCustomerPartitions.contains(path._1(0).head.getName()))
        customerCache = customerCache.union(customerSample)

      if (orderCache.isEmpty)
        orderCache = orderSample
      else if (!cachedOrderPartitions.contains(path._1(1).head.getName()))
        orderCache = orderCache.union(orderSample)

      if (lineitemCache.isEmpty)
        lineitemCache = lineitemSample
      else if (!cachedLineitemPartitions.contains(path._1(2).head.getName()))
        lineitemCache = lineitemCache.union(lineitemSample)


      cachedCustomerPartitions += path._1(0).head.getName()
      cachedOrderPartitions += path._1(1).head.getName()
      cachedLineitemPartitions += path._1(2).head.getName()

      customerCache.createOrReplaceTempView("customer")
      orderCache.createOrReplaceTempView("order")
      lineitemCache.createOrReplaceTempView("lineitem")

      val partial1 = spark.sql(query)

      val partialResMap1 = Map[List[String], java.math.BigDecimal]()
      partial1.collect().foreach(row => partialResMap1 += List(row.getAs("o_orderdate").toString)
        -> row.getAs[java.math.BigDecimal]("avg_revenue"))

      val errorsTemp: Map[List[String], Double] = Map()

      for ((keyPartial, valuePartial) <- partialResMap1){
        val trueValue = fullResMap(keyPartial).toDouble
        val est = valuePartial.toString.toDouble
        val error = math.abs(est - trueValue) / trueValue
        errorsTemp += (keyPartial -> error)
      }


      var numGroups = 0
      var avgError = 0.0

      for ((group,error) <- errorsTemp) {
        avgError += error
        numGroups += 1
      }
      if (numGroups == 0)
        avgError = 1.0
      else
        avgError = avgError / numGroups.toDouble

      val a = cachedCustomerPartitions.distinct.size.toDouble
      val b = cachedOrderPartitions.distinct.size.toDouble
      val c = cachedLineitemPartitions.distinct.size.toDouble
      val total = ((a + b + c) / 76.0) * 100

      print(numGroups + ",")
      print(avgError + ",")
      print(total)
      println()


      //println(numGroups, avgError)


      // println(group.mkString(",") + "," + error)



    }
  }


  def doOtherStuff2(spark: SparkSession): Unit = {
    val query = Query.BenchQueries.QuerySevenAvg
    val Stats = new StatisticsBuilder(spark)

    val qp = new QueryParser
    val joinInputs = qp.parseQueryJoin(spark, query)

    val fullResMap = Map[List[String], String]()
    val full = spark.read.option("header", true).csv("spark-warehouse/q7avg_exactsf10_skew.csv")
    full.collect().foreach(row => fullResMap += List(row.getAs("o_orderdate").toString)
      -> row.getAs[String]("avg_revenue"))

    /*
    Stats.createCMS(spark, "customer.parquet", "c_custkey")
    Stats.createCMS(spark, "order.parquet", "o_custkey")
    Stats.createCMS(spark, "order.parquet", "o_orderkey")
    Stats.createCMS(spark, "lineitem.parquet", "l_orderkey")
    Stats.calculateMeasure("customer.parquet", "c_custkey", "mean")
    Stats.calculateMeasure("customer.parquet", "c_custkey", "stdError")
    Stats.calculateMeasure("order.parquet", "o_custkey", "mean")
    Stats.calculateMeasure("order.parquet", "o_custkey", "stdError")
    Stats.calculateMeasure("order.parquet", "o_orderkey", "mean")
    Stats.calculateMeasure("order.parquet", "o_orderkey", "stdError")

    Stats.calculateMeasure("lineitem.parquet", "l_orderkey", "mean")
    Stats.calculateMeasure("lineitem.parquet", "l_orderkey", "stdError")
     */
    //Stats.calculateMeasure("order.parquet", "o_orderdate", "countUnique")

    // Idea: Store all cms objects in one mega object, for faster loading
    Stats.loadCMS()

    val custPartitions = getListOfFiles(new File("Statistics/customer.parquet"), excludedFiles)
    val orderPartitions = getListOfFiles(new File("Statistics/order.parquet"), excludedFiles)
    val lineitemPartitions = getListOfFiles(new File("Statistics/lineitem.parquet"), excludedFiles)


    val joinGraph = new JoinGraphV2()

    for (cp <- custPartitions) {
      val vertex = new joinGraph.Node(cp.getName, "start", "customer.parquet")
      joinGraph.addVertex(vertex)
    }

    for (op <- orderPartitions) {
      val vertex = new joinGraph.Node(op.getName, "interm", "order.parquet")
      joinGraph.addVertex(vertex)
    }

    for (lp <- lineitemPartitions) {
      val vertex = new joinGraph.Node(lp.getName, "end", "lineitem.parquet")
      joinGraph.addVertex(vertex)
    }

    joinGraph.addEdges("customer.parquet", "order.parquet")
    joinGraph.addEdges("order.parquet", "lineitem.parquet")

    val paths: ListBuffer[ListBuffer[Set[joinGraph.Node]]] = joinGraph.calculatePaths()

    val weightedPaths: Map[ListBuffer[Set[joinGraph.Node]], Double] = Map()
    // TODO: change to calculate cms inner product only once for each unique combination
    // IDEA: maybe use some decision tree?

    for (path <- paths){
      var size = 1

      for (i <- 0 to path.length - 2){
        val leftNodeName = path(i).head.getName()
        val leftNodeKey = joinInputs(i)._3
        val leftNodeTable = path(i).head.getTableName()
        val rightNodeName = path(i+1).head.getName()
        val rightNodeKey = joinInputs(i)._4
        val rightNodeTable = path(i+1).head.getTableName()

        val cmsLeft = Stats.getCMS(leftNodeTable, leftNodeName, leftNodeKey)
        val cmsRight = Stats.getCMS(rightNodeTable, rightNodeName, rightNodeKey)
        val joinSize = cmsLeft.innerProduct(cmsRight).estimate.toDouble

        if (joinSize == 0)
          size = 0
      }

      // assign weights better
      if (size == 1){
        val orderStatsOrderdate = Stats.getStatsAsMap("Statistics/" +  path(1).head.getTableName + "/" +
          path(1).head.getName() + "/o_orderdate/statistics.txt")
        weightedPaths += (path -> orderStatsOrderdate("countUnique").toDouble)
      }

    }

    // sample paths
    val samplePaths: ListBuffer[ListBuffer[Set[joinGraph.Node]]] = ListBuffer()

    for (i <- 0 to 5){
      val path = sample(weightedPaths)
      joinGraph.printPath(path)
      weightedPaths -= path
      samplePaths += path
    }


    var customerCache = spark.emptyDataFrame
    var orderCache = spark.emptyDataFrame
    var lineitemCache = spark.emptyDataFrame
    var cachedCustomerPartitions: ListBuffer[String] = ListBuffer()
    var cachedOrderPartitions: ListBuffer[String] = ListBuffer()
    var cachedLineitemPartitions: ListBuffer[String] = ListBuffer()

    // TODO: If universe partition was already sampled, sample from different space

    for (path <- samplePaths) {

      // joinGraph.printPath(path._1)
      var customerSample = spark.read.parquet("data_parquet/" + path(0).head.getTableName() + "/" +
        path(0).head.getName())
      var orderSample = spark.read.parquet("data_parquet/" + path(1).head.getTableName() + "/" +
        path(1).head.getName())
      val lineitemSample = spark.read.parquet("data_parquet/" + path(2).head.getTableName() + "/" +
        path(2).head.getName())

      if (customerCache.isEmpty)
        customerCache = customerSample
      else if (!cachedCustomerPartitions.contains(path(0).head.getName()))
        customerCache = customerCache.union(customerSample)

      if (orderCache.isEmpty)
        orderCache = orderSample
      else if (!cachedOrderPartitions.contains(path(1).head.getName()))
        orderCache = orderCache.union(orderSample)

      if (lineitemCache.isEmpty)
        lineitemCache = lineitemSample
      else if (!cachedLineitemPartitions.contains(path(2).head.getName()))
        lineitemCache = lineitemCache.union(lineitemSample)


      cachedCustomerPartitions += path(0).head.getName()
      cachedOrderPartitions += path(1).head.getName()
      cachedLineitemPartitions += path(2).head.getName()

      customerCache.createOrReplaceTempView("customer")
      orderCache.createOrReplaceTempView("order")
      lineitemCache.createOrReplaceTempView("lineitem")

      val partial1 = spark.sql(query)

      val partialResMap1 = Map[List[String], java.math.BigDecimal]()
      partial1.collect().foreach(row => partialResMap1 += List(row.getAs("o_orderdate").toString)
        -> row.getAs[java.math.BigDecimal]("avg_revenue"))

      val errorsTemp: Map[List[String], Double] = Map()

      for ((keyPartial, valuePartial) <- partialResMap1){
        val trueValue = fullResMap(keyPartial).toDouble
        val est = valuePartial.toString.toDouble
        val error = math.abs(est - trueValue) / trueValue
        errorsTemp += (keyPartial -> error)
      }


      var numGroups = 0
      var avgError = 0.0

      for ((group,error) <- errorsTemp) {
        avgError += error
        numGroups += 1
      }
      if (numGroups == 0)
        avgError = 1.0
      else
        avgError = avgError / numGroups.toDouble

      val a = cachedCustomerPartitions.distinct.size.toDouble
      val b = cachedOrderPartitions.distinct.size.toDouble
      val c = cachedLineitemPartitions.distinct.size.toDouble
      val total = ((a + b + c) / 76.0) * 100

      print(numGroups + ",")
      print(avgError + ",")
      println()
    }
  }

  def test(): Unit ={
    val dist = Map("a" -> 200.0, "b" -> 300.0, "c" -> 10000.0)
    val temp = sample(dist)
    println(temp)
  }

  final def sample[A](dist: scala.collection.mutable.Map[A, Double]): A = {
    val p = scala.util.Random.nextDouble * dist.values.sum
    val it = dist.iterator
    var accum = 0.0
    while (it.hasNext) {
      val (item, itemProb) = it.next
      accum += itemProb
      if (accum >= p)
        return item // return so that we don't have to search through the whole distribution
    }
    sys.error(f"Sampling error") // needed so it will compile
  }

}
