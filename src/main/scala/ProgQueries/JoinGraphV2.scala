package ProgQueries

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class JoinGraphV2 {

  private var adjMap: mutable.Map[Node, List[Node]] = mutable.Map()
  private val startNodes: ListBuffer[Node] = ListBuffer.empty
  private val endNodes: ListBuffer[Node] = ListBuffer.empty
  private var storedPaths: Set[ListBuffer[Node]] = Set()
  private var nodeMap: Map[String, Node] = Map()


  class Path(nodes: ListBuffer[Set[Node]]) {
    private val ZScore = 0.0
  }


  class Node(var nName: String, var nType: String, var tName: String) {
    private val nodeName: String = nName  // partition name
    private val nodeType: String = nType
    private val tableName: String = tName


    def getName(): String = {
      nodeName
    }

    def getNodeType(): String = {
      nodeType
    }

    def getTableName(): String = {
      tableName
    }

  }


  def addVertex(v: Node): Unit ={
    adjMap += v -> List.empty
    nodeMap += (v.getNodeType() -> v)

    if (v.getNodeType == "start")
      startNodes += v
    else if (v.getNodeType == "end")
      endNodes += v
  }


  def addEdge(v: Node, w: Node): Unit ={
    adjMap = adjMap + (v -> (adjMap.getOrElse(v, List.empty) :+ w))
  }


  def addEdges(sourceTable: String, destTable: String): Unit ={
    for ((k1,v1) <- adjMap){
      for ((k2,v2) <- adjMap){
        if (k1.tName == sourceTable && k2.tName == destTable && !v1.exists(x => x.getName() == k2.getName())) {
          addEdge(k1, k2)
        }
      }
    }
  }


  def calculatePaths(): ListBuffer[ListBuffer[Set[Node]]] ={

    var allPaths: ListBuffer[ListBuffer[Node]] = ListBuffer.empty

    for (i <- startNodes.indices) {
      for (j <- endNodes.indices){
        var visited: mutable.Map[Node, Boolean] = mutable.Map()
        for ((k,v) <- adjMap){
          visited += k -> false
        }

        val start = startNodes(i)
        val end =  endNodes(j)

        var currentPath: ListBuffer[Node] = ListBuffer.empty
        currentPath += start

        allPaths = allPaths ++ DFS(start, end, visited, currentPath, ListBuffer.empty)
      }
    }

    val allPathsSet = allPaths.toSet
    val newPaths = allPathsSet.diff(storedPaths)
    storedPaths = storedPaths.union(newPaths)

    // path merging algorithm
    val pathStore: ListBuffer[ListBuffer[Set[Node]]] = ListBuffer.empty

    for (path <- newPaths){
      val tempPath: ListBuffer[Set[Node]] = ListBuffer.empty
      for (node <- path){
        tempPath.append(Set(node))
      }
      pathStore.append(tempPath)
    }

    pathStore

  }


  def printPath(path: ListBuffer[Set[Node]]): Unit = {
    println()
    for (v <- path) {
      print("{")
      for (i <- v)
        print(i.getName())
      print("}")
    }
    println()
  }


  def DFS(u: Node, w: Node, visited: mutable.Map[Node, Boolean], currentPath: ListBuffer[Node],
          currentPaths: ListBuffer[ListBuffer[Node]]): ListBuffer[ListBuffer[Node]] ={

    visited(u) = true

    if (u.getName() == w.getName()) {
      currentPaths.append(currentPath.clone())
    }

    for (v <- adjMap(u)){
      if (!visited(v)){
        currentPath += v
        DFS(v, w, visited, currentPath, currentPaths)
        currentPath -= v
      }
    }

    visited(u) = false

    return currentPaths
  }


  def countNodes(): Integer ={
    return adjMap.size
  }


  def printGraph(): Unit ={
    for ((k,v) <- adjMap){
      println()
      print(k.getName() + " -> [")
      for (vi <- v){
        print(vi.getName() + ", ")
      }
      print("]")
      println()
    }
  }


}
