package PartitionLogic

import scala.collection.mutable

class Partition(val pName: String, val pTable: String, val pSize: Long) {
  private val partitionName = pName
  private val partitionTable = pTable
  private var selectivity: Double = 0.0
  private val cardinalities: mutable.Map[String, Double] = mutable.Map()
  private var heavyHitters: List[String] = List()
  private var outlierScore = 0.0
  private val partSize = pSize

  def getPartititionName(): String = {
    partitionName
  }

  def getTableName(): String = {
    partitionTable
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

  def setHeavyHitters(hh: List[String]): Unit = {
    heavyHitters = hh
  }

  def getHeavyHitters(): List[String] = {
    heavyHitters
  }

  def setOutlierScore(score: Double): Unit = {
    outlierScore = score
  }

  def getOutlierScore(): Double = {
    outlierScore
  }

  def getPartSize(): Long = {
    partSize
  }

}
