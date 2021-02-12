package ProgQueries

import java.io.File

class QueryRewriter {

  def rewriteToUnion(query: String, samplePaths: List[File], tableName: String, cols: Array[String]): String ={
    var newFrom = ""
    val firstChar = tableName.take(1)
    val colsFiltered = cols.filter(_.startsWith(firstChar))
    val samplePercent = samplePaths.length

    if (samplePercent == 1)
      newFrom = " parquet.`" + samplePaths.head + "`"
    else {
      for (i <- 0 until samplePercent){
        newFrom = newFrom + "select " + colsFiltered.mkString(",") + " from " + " parquet.`" + samplePaths(i) + "`"
        if (i < samplePercent - 1)
          newFrom = newFrom + " union "
      }
    }

    newFrom = "(" + newFrom + ")"

    return query.replaceFirst(" " + tableName, newFrom + " as " + tableName)

  }


  def extractColumnsFromQuery(query: String): Array[String] ={
    val fromIndex = query indexOf "from"
    var cols = query.substring(7, fromIndex).trim.split(",")

    cols = cols.map(_.replaceAll("avg|sum|count|\\(|\\)|\\s", ""))

    return cols

  }

}
