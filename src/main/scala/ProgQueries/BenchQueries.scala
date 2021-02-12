package ProgQueries

object BenchQueries extends Enumeration {

  // Single table
  val QueryOne = """SELECT L_RETURNFLAG, L_LINESTATUS, SUM(L_QUANTITY) AS SUM_QTY,
            | SUM(L_EXTENDEDPRICE) AS SUM_BASE_PRICE, SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS SUM_DISC_PRICE,
            | AVG(L_EXTENDEDPRICE) AS AVG_PRICE, AVG(L_DISCOUNT) AS AVG_DISC, COUNT(*) AS COUNT_ORDER
            |FROM LINEITEM
            |GROUP BY L_RETURNFLAG, L_LINESTATUS""".stripMargin.toLowerCase()

  // 2 table join
  val QueryTwo = """SELECT L_ORDERKEY, SUM((L_EXTENDEDPRICE*(1-L_DISCOUNT))) AS SUM_REVENUE, O_SHIPPRIORITY
                   |FROM ORDER , LINEITEM
                   |WHERE L_ORDERKEY = O_ORDERKEY
                   |GROUP BY L_ORDERKEY, O_SHIPPRIORITY""".stripMargin.toLowerCase()


  // 6 table join
  val QueryThree = """SELECT N_NAME, SUM((L_EXTENDEDPRICE*(1-L_DISCOUNT))) AS SUM_REVENUE
                     |FROM CUSTOMER, ORDER, LINEITEM, SUPPLIER, NATION, REGION
                     |WHERE C_CUSTKEY = O_CUSTKEY AND L_ORDERKEY = O_ORDERKEY AND L_SUPPKEY = S_SUPPKEY
                     |AND C_NATIONKEY = S_NATIONKEY AND S_NATIONKEY = N_NATIONKEY AND N_REGIONKEY = R_REGIONKEY
                     |GROUP BY N_NAME""".stripMargin.toLowerCase()


  // Single table
  val QueryFour = """SELECT SUM(L_EXTENDEDPRICE*L_DISCOUNT) AS SUM_REVENUE
                    |FROM LINEITEM
                    |WHERE L_SHIPDATE >= '1994-01-01'
                    |AND L_DISCOUNT BETWEEN .06 - 0.01 AND .06 + 0.01 AND L_QUANTITY < 24""".stripMargin.toLowerCase()


  // 2 table
  val QueryFive = """SELECT SUM(L_EXTENDEDPRICE) AS AVG_YEARLY FROM LINEITEM
                    |JOIN PART ON LINEITEM.L_PARTKEY = PART.P_PARTKEY
                    |WHERE L_QUANTITY < (SELECT 0.2*AVG(L_QUANTITY) FROM LINEITEM WHERE L_PARTKEY = P_PARTKEY)"""
    .stripMargin.toLowerCase()

  // 3 table
  val QuerySix = """SELECT L_ORDERKEY, SUM((L_EXTENDEDPRICE*(1-L_DISCOUNT))) AS SUM_REVENUE, O_ORDERDATE, O_SHIPPRIORITY
                   |FROM CUSTOMER JOIN ORDER ON CUSTOMER.C_CUSTKEY = ORDER.O_CUSTKEY
                   |JOIN LINEITEM ON LINEITEM.L_ORDERKEY = ORDER.O_ORDERKEY
                   |WHERE O_ORDERDATE < '1995-03-15'
                   |GROUP BY L_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY""".stripMargin.toLowerCase()
}
