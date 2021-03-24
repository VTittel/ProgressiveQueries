package ProgQueries

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class JoinGraph {

  private var adjMap: mutable.Map[Node, List[Node]] = mutable.Map()
  private var startNodes: ListBuffer[Node] = ListBuffer.empty
  private var endNodes: ListBuffer[Node] = ListBuffer.empty
  private var storedPaths: Set[ListBuffer[Node]] = Set()


  class Node(var nName: String, var nType: String, var sType: String, var tName: String, var iter: Integer) {
    private val nodeName: String = nName  // path
    private val nodeType: String = nType
    private val sampleType: String = sType
    private val tableName: String = tName
    private val iteration: Integer = iter // at which iteration the node was added


    def getName(): String = {
      return nodeName
    }

    def getNodeType(): String = {
      return nodeType
    }

    def getSampleType(): String = {
      return sampleType
    }

    def getTableName(): String = {
      return tableName
    }

    def getIteration(): Integer = {
      return iteration
    }
  }


  def addVertex(v: Node): Unit ={
    adjMap += v -> List.empty

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
        if (k1.tName == sourceTable && k2.tName == destTable && !v1.exists(x => x.getName() == k2.getName()) &&
          !(k1.getSampleType() == "univ" && k2.getSampleType() == "univ" && k1.getIteration() != k2.getIteration())) {
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
    var pathStore: ListBuffer[ListBuffer[Set[Node]]] = ListBuffer.empty

    for (path <- newPaths){
      val tempPath: ListBuffer[Set[Node]] = ListBuffer.empty
      for (node <- path){
        tempPath.append(Set(node))
      }
      pathStore.append(tempPath)
    }

    val pLen = newPaths.head.length


    // TODO : comment it
    /*
    For each path P, check if its node at position i can be merged with the node at position i of another path P'.
    In order to be eligible for a merge, P \ P(i) == P' \ P'(i) must hold true. That is, the paths must be exactly
    the same except for the node at position i. Keep going through the paths iteratively, updating them if a merge
    has occured. Old paths are removed, and the new merged path is added.
     */
    for (i <- 0 until pLen){
      var pathStoreCopy = pathStore.clone()

      for (pathOuter <- pathStoreCopy) {  // For each path P
        // Store the nodes at i
        val prevNodesOuter = pathOuter(pathOuter.length-i-1)
        val newPathOuter = pathOuter.clone()
        // Remove the node temporarily, to enable comparison
        newPathOuter -= newPathOuter(newPathOuter.length-i-1)
        var tempPathStore = pathStoreCopy.clone()
        tempPathStore -= pathOuter

        for (pathInner <- tempPathStore) {  // For each path P'
          // Same as above
          val prevNodesInner = pathInner(pathInner.length-i-1)
          val newPathInner = pathInner.clone()
          newPathInner -= newPathInner(newPathInner.length-i-1)
          // If the paths are equal, remove them from original list and add the merged paths
          if (newPathInner.equals(newPathOuter)) {
            pathStore -= pathOuter
            pathStore -= pathInner
            pathStoreCopy -= pathInner
            pathStoreCopy -= pathOuter
            val updatedPath = newPathOuter.clone()
            updatedPath.insert(pathInner.length-i -1, prevNodesInner.union(prevNodesOuter))
            pathStore.append(updatedPath)
            pathStoreCopy.append(updatedPath)
          }
        }
      }
    }

    return pathStore

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
