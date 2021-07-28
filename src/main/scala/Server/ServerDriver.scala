package Server

import org.eclipse.jetty.server.handler.{AbstractHandler, ContextHandler, HandlerCollection}
import org.eclipse.jetty.server.{Request, Server}

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import java.util.concurrent.{Callable, Executors, FutureTask}
import concurrent.{ExecutionContext, ExecutionContextExecutor, Future, Promise, future}
import java.util.Date
import org.apache.spark.sql.SparkSession

import scala.util.parsing.json._
import scala.collection.mutable
import scala.collection.mutable.Map

/*
Shitty API:

Start query: localhost:8080/StartQuery with body {"query" : "your query here"}
Important! Always alias aggregates, with aggregate name in the alias e.g.
{"query" : "select avg(o_totalprice) as avg_tp, sum(o_totalprice) as sum_tp, o_orderstatus
            from order where o_orderpriority = '5-LOW' group by o_orderstatus"}

Get query result: localhost:8080/GetQueryResult?qID = your query id
Returns a map, where the keys are a tuple in the form (AGG, GROUP)

TODO: Implement stopping of queries
 */

object ServerDriver {

  var queryStorage: mutable.Map[String, Any] = scala.collection.mutable.Map[String,Any]()

  var futureStorage: mutable.Map[String, Any] = scala.collection.mutable.Map[String,Any]()

  class QueryRunner(spark: SparkSession) extends AbstractHandler {

    class Cancellable[T](executionContext: ExecutionContext, todo: => T) {

      private val jf: FutureTask[T] = new FutureTask[T](
        new Callable[T] {
          override def call(): T = todo
        }
      )

      executionContext.execute(jf)

      implicit val _: ExecutionContext = executionContext

      val future: Future[T] = Future {
        jf.get
      }

      def cancel(): Unit = jf.cancel(true)

    }

    object Cancellable {
      def apply[T](todo: => T)(implicit executionContext: ExecutionContext): Cancellable[T] =
        new Cancellable[T](executionContext, todo)
    }

    override def handle(target: String, req: Request, httpReq: HttpServletRequest, httpRes: HttpServletResponse)
    : Unit = {

      var qID = util.hashing.MurmurHash3.stringHash(new Date().getTime.toString)
      if (qID < 0)
        qID = qID * -1

      httpRes.setContentType("text/html")
      httpRes.setStatus(HttpServletResponse.SC_OK)
      httpRes.getWriter.println("Query ID : " + qID)
      req.setHandled(true)


      val requestBody: collection.immutable.Map[String, Any] = JSON.parseFull(req.getReader.readLine())
        .getOrElse(collection.immutable.Map[String,Any]())
        .asInstanceOf[collection.immutable.Map[String,Any]].collect{
        case e : (String, Any) if(e._2 != null && e._2.toString.nonEmpty) => e
      }

      val query = requestBody("query").toString

      implicit val context: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

      var future = Cancellable{
        runQuery(spark, query, qID.toString)
        println("Running asynchronously on another thread")
      }

    }
  }



  class QueryResult() extends AbstractHandler {

    override def handle(target: String,
                        req: Request,
                        httpReq: HttpServletRequest,
                        httpRes: HttpServletResponse): Unit = {

      httpRes.setContentType("text/html")
      httpRes.setStatus(HttpServletResponse.SC_OK)
      val qID = req.getParameter("qID")
      httpRes.getWriter.println(queryStorage(qID).toString)
      req.setHandled(true)

    }
  }

  def startSparkServer(): SparkSession = {
    val spark = SparkSession.builder()
      .appName("Spark SQL basic example")
      //.config("spark.master", "spark://mcs-computeA002:7077")
      .config("spark.master", "local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    return spark
  }


  def runQuery(spark: SparkSession, query: String, qID: String): Unit ={
    //TODO: Take params from user
    val params = Map("errorTol" -> "2.0",
      "samplePercent" -> "10",
      "b" -> "100",
      "dataDir" -> "data_parquet_sf1//",
      "alpha" -> "0.05")

    val queryFormatted = query.stripMargin
    SingleTableQueryExecutorTest.runSingleTableQuery(spark, queryFormatted, params("dataDir"), params("alpha").toDouble,
      queryStorage, qID)
  }


  def main(args: Array[String]): Unit = {
    val server = new Server(8080)
    val spark = startSparkServer()

    val handlerCollection = new HandlerCollection()

    val startQueryContext = new ContextHandler()
    startQueryContext.setContextPath("/StartQuery")
    startQueryContext.setAllowNullPathInfo(true)
    startQueryContext.setResourceBase(".")
    startQueryContext.setHandler(new QueryRunner(spark))
    startQueryContext.setClassLoader(Thread.currentThread.getContextClassLoader)
    handlerCollection.addHandler(startQueryContext)

    val QueryResultContext = new ContextHandler()
    QueryResultContext.setContextPath("/GetQueryResult")
    QueryResultContext.setAllowNullPathInfo(true)
    QueryResultContext.setResourceBase(".")
    QueryResultContext.setHandler(new QueryResult())
    QueryResultContext.setClassLoader(Thread.currentThread.getContextClassLoader)
    handlerCollection.addHandler(QueryResultContext)

    server.setHandler(handlerCollection)
    server.start()
    server.join()
  }


}
