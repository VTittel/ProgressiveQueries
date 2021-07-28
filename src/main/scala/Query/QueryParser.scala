package Query

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.expressions._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class QueryParser {

  def stringifyExpressions(expression: Expression): Seq[String] = {
    expression match{
      case And(l,r) => (l,r) match {
        case (gte: GreaterThanOrEqual,lte: LessThanOrEqual) => Seq(s"""${gte.left.toString} between ${gte.right.toString} and ${lte.right.toString}""")
        case (_,_) => Seq(l,r).flatMap(stringifyExpressions)
      }
      case Or(l,r) => Seq(Seq(l,r).flatMap(stringifyExpressions).mkString("(",") OR (", ")"))
      case eq: EqualTo => Seq(s"${eq.left.toString} = ${eq.right.toString}")
      case inn: IsNotNull => Seq(s"${inn.child.toString} is not null")
      case p: Predicate => Seq(p.toString)
    }
  }


  def parseQueryFilter(spark: SparkSession, query: String): Unit ={
    val logicalPlan = spark.sessionState.sqlParser.parsePlan(query)

    // Outer listbuffer contains groups of expressions. Each group has its own listbuffer
    val exprs: ListBuffer[ListBuffer[(String, String, String)]] = ListBuffer()
    val topLevelOp = logicalPlan.collect {case f: Filter => f.condition.prettyName}.head
    // if toplevelop is AND or OR, that means we have multiple clauses in where, otherwise only one
   // if (!(topLevelOp == "or" || topLevelOp == "and"))


    /*

val logicalPlan = spark.sessionState.sqlParser.parsePlan(query)

val clauses: ListBuffer[ListBuffer[(String, String, String)]] = ListBuffer()
val intraOps: ListBuffer[String] = ListBuffer()
val topNode = logicalPlan.collect {case f: Filter => f.condition}.head
// if toplevelop is AND or OR, that means we have multiple clauses in where, otherwise only one
if (!(topNode.prettyName == "or" || topNode.prettyName == "and")) {
  val leaves = topNode.collectLeaves()
  val temp: ListBuffer[(String, String, String)] = ListBuffer()
  // left operator right
  temp.append((leaves(0).toString(), leaves(1).toString(), topNode.prettyName))
  clauses.append(temp)
} else {
  val predicates: Seq[Expression] = logicalPlan.collect{case f: Filter =>
    f.condition.productIterator.flatMap{
      case o:Predicate => Seq(o)
      case Or(l,r) => Seq(l,r)
      case And(l,r) => Seq(l,r)
    }
  }.toList.flatten

  println(predicates)

  //for (pred <- predicates) {
 //   if (topNode.prettyName == "or" || topNode.prettyName == "and") {

  //  }
}

 */
  }

  /*
  Node type:
  r: Aggregate => r.condition for join key, r.collectLeaves for tables
  */
  def parseQueryJoin(spark: SparkSession, query: String): ArrayBuffer[(String, String, String, String)] ={
    val logicalPlan = spark.sessionState.sqlParser.parsePlan(query)

    val result = ArrayBuffer[(String, String, String, String)]()

    val ast = logicalPlan.collect {case r: Join => (r.condition.get.collectLeaves(),r.collectLeaves())}
    var i = 0

    for (aggNode <- ast.reverse) {
      val joinCondLeft = aggNode._1.head.toString()
      val joinKeyLeft = joinCondLeft.substring(joinCondLeft.indexOf(".")+1, joinCondLeft.length)
      val joinCondRight = aggNode._1(1).toString()
      val joinKeyRight = joinCondRight.substring(joinCondRight.indexOf(".")+1, joinCondRight.length)

      val joinTables = aggNode._2
      var leftTable = joinTables(i).toString()
      leftTable = leftTable.substring(leftTable.indexOf("`")+1, leftTable.lastIndexOf("`"))
      var rightTable = joinTables(i+1).toString()
      rightTable = rightTable.substring(rightTable.indexOf("`")+1, rightTable.lastIndexOf("`"))

      result.append((leftTable, rightTable, joinKeyLeft, joinKeyRight))

      i += 1
    }

    result
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
    result
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


  /* Extract tables from query */
  def getTables(spark: SparkSession, query: String): Seq[String] = {
    val logicalPlan = spark.sessionState.sqlParser.parsePlan(query)
    import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
    logicalPlan.collect { case r: UnresolvedRelation => r.tableName}
  }


}
