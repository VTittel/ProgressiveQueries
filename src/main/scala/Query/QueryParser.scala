package Query

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join, LogicalPlan, Project}

import scala.collection.mutable.ArrayBuffer

class QueryParser {

  /*
  Node type:
  r: Aggregate => r.condition for join key, r.collectLeaves for tables
  */
  def parseQueryJoin(spark: SparkSession, query: String): ArrayBuffer[(String, String, String)] ={
    val logicalPlan = spark.sessionState.sqlParser.parsePlan(query)

    val result = ArrayBuffer[(String, String, String)]()

    val ast = logicalPlan.collect {case r: Join => (r.condition.get.collectLeaves(),r.collectLeaves())}
    var i = 0

    for (aggNode <- ast.reverse) {
      val joinCond = aggNode._1.head.toString()
      val joinKey = joinCond.substring(joinCond.indexOf("_")+1, joinCond.length)

      val joinTables = aggNode._2
      var leftTable = joinTables(i).toString()
      leftTable = leftTable.substring(leftTable.indexOf("`")+1, leftTable.lastIndexOf("`"))
      var rightTable = joinTables(i+1).toString()
      rightTable = rightTable.substring(rightTable.indexOf("`")+1, rightTable.lastIndexOf("`"))

      result.append((leftTable, rightTable, joinKey))

      i += 1
    }

    return result
  }


  /*
  Node type:
  r: Aggregate => r.aggregateExpressions
  r: Project => r.projectList works for when there is no group by
  */
  def parseQueryAggregate(logicalPlan: LogicalPlan, hasGroupBy: Boolean): ArrayBuffer[(String, String, String)] ={

    val result = ArrayBuffer[(String, String, String)]()
    var ast = Seq[Seq[NamedExpression]]()

    if (hasGroupBy)
      ast = logicalPlan.collect { case r: Aggregate => r.aggregateExpressions}
    else
      ast = logicalPlan.collect { case r: Project => r.projectList}

    for (aggNode <- ast.head) {
      if (aggNode.children.nonEmpty) {
        val aggString = aggNode.children.head.toString().replaceAll("'|\\s", "")
        val braceIndex = aggString indexOf "("
        val aggFunc = aggString.substring(0, braceIndex)
        var aggCol = aggString.substring(braceIndex, aggString.length).replaceAll("\\s", "")
        aggCol =  aggCol.substring(aggCol.indexOf("(")+1, aggCol.lastIndexOf(")"))
        val aggNodeStr = aggNode.toString()
        val alias = aggNodeStr.substring((aggNodeStr indexOf "AS") + 3, aggNodeStr indexOf "#")
        result.append((aggFunc, aggCol, alias))
      }
    }
    return result
  }


  /*
  Node type:
  r: Aggregate => r.groupingExpressions
  */
  def parseQueryGrouping(logicalPlan: LogicalPlan): Seq[String] ={

    val ast = logicalPlan.collect { case r: Aggregate => r.groupingExpressions}

    if (ast.isEmpty)
      return Seq()

    val resultAst = ast.head.map(col => col.toString().replaceAll("'|\\s", ""))
    return resultAst
  }

}
