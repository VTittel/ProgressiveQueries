package PartitionLogic

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class JoinPath[A](val pathNodes: ListBuffer[Set[A]], val joinGraph: JoinGraphV2) {
  private val nodes: ListBuffer[Set[joinGraph.Node]] = pathNodes.asInstanceOf[ListBuffer[Set[joinGraph.Node]]]
  private var selectivity: Double = 0.0
  private val cardinalities: mutable.Map[String, Double] = mutable.Map()
  private var heavyHitters: Map[(String, String), List[String]] = Map()
  private var allLocalHeavyHitters: List[String] = List()
  private var outlierScore = 0.0
  private val tupleCounts: mutable.Map[String, Long] = mutable.Map()

  def getNodes(): ListBuffer[Set[joinGraph.Node]] = {
    nodes
  }

  def setSelectivity(sel: Double): Unit ={
    selectivity = sel
  }

  def getSelectivity(): Double = {
    selectivity
  }

  def setCardinality(col: String, card: Double): Unit = {
    cardinalities += (col -> card)
  }

  def getCardinality(col: String): Double = {
    cardinalities(col)
  }

  def setHeavyHitters(tableName: String, colName: String, hh: List[String]): Unit = {
    heavyHitters += ((tableName, colName) -> hh)
  }

  def getHeavyHitters(tableName: String, colName: String): List[String] = {
    heavyHitters((tableName, colName))
  }

  def setAllLocalHeavyHitters(hh: List[String]): Unit ={
    allLocalHeavyHitters = hh
  }

  def getAllLocalHeavyHitters(): List[String] ={
    allLocalHeavyHitters
  }

  def setOutlierScore(score: Double): Unit = {
    outlierScore = score
  }

  def getOutlierScore(): Double = {
    outlierScore
  }

  def setTupleCount(partition: String, numTuples: Long): Unit = {
    tupleCounts += (partition -> numTuples)
  }

  def getTupleCount(partition: String): Long ={
    tupleCounts(partition)
  }

}
