package Query

object BenchQueries extends Enumeration {

  // Single table

  val Query1 =
    """select
      |l_returnflag,
      |l_linestatus,
      |sum(l_quantity) as sum_qty,
      |sum(l_extendedprice) as sum_base_price,
      |sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
      |avg(l_quantity) as avg_qty,
      |avg(l_extendedprice) as avg_price,
      |avg(l_discount) as avg_disc
      |from
      |lineitem
      |where
      |l_shipdate < '1998-12-01'
      |group by
      |l_returnflag,
      |l_linestatus""".stripMargin


  val Query3 =
    """select
      |sum(l_extendedprice*(1-l_discount)) as sum_revenue,
      |o_orderdate,
      |o_shippriority,
      |count(o_orderdate) as count_tuples,
      |1 + floor(rand()*100) as c_sid,
      |1 + floor(rand()*100) as o_sid,
      |1 + floor(rand()*100) as l_sid
      |from
      |customer join order on customer.c_custkey = order.o_custkey
      |join lineitem on order.o_orderkey = lineitem.l_orderkey
      |where
      |c_mktsegment = 'FURNITURE'
      |and o_orderdate < '1995-03-15'
      |and l_shipdate > '1995-03-17'
      |group by
      |o_orderdate,
      |o_shippriority""".stripMargin

  val Query4 =
    """
      |select
      |o_orderpriority,
      |count(l_commitdate) as count_orders,
      |count(l_commitdate) as count_tuples,
      |1 + floor(rand()*100) as o_sid,
      |1 + floor(rand()*100) as l_sid
      |from order join lineitem on
      |order.o_orderkey = lineitem.l_orderkey
      |where l_commitdate < '1996-01-06'
      |group by
      |o_orderpriority
      |""".stripMargin

  val Query5 =
    """
      |select
      |sum(l_extendedprice*(1-l_discount)) as sum_revenue,
      |n_name,
      |count(o_orderdate) as count_tuples,
      |1 + floor(rand()*100) as c_sid,
      |1 + floor(rand()*100) as o_sid,
      |1 + floor(rand()*100) as l_sid,
      |1 + floor(rand()*100) as s_sid,
      |1 + floor(rand()*100) as n_sid,
      |1 + floor(rand()*100) as r_sid
      |from customer join order on customer.c_custkey = order.o_custkey
      |join lineitem on order.o_orderkey = lineitem.l_orderkey
      |join supplier on lineitem.l_suppkey = supplier.s_suppkey
      |join nation on supplier.s_nationkey = nation.n_nationkey
      |join region on nation.n_regionkey = region.r_regionkey
      |where
      |r_name = 'AMERICA'
      |and o_orderdate < '1994-01-01'
      |group by n_name
      |""".stripMargin

  val Query10 =
    """
      |select /*+ BROADCAST(nation) */
      |sum(l_extendedprice*(1-l_discount)) as sum_revenue,
      |n_name,
      |l_shipmode,
      |c_nationkey,
      |count(o_orderdate) as count_tuples,
      |1 + floor(rand()*100) as c_sid,
      |1 + floor(rand()*100) as o_sid,
      |1 + floor(rand()*100) as l_sid,
      |1 + floor(rand()*100) as n_sid
      |from nation join customer on nation.n_nationkey = customer.c_nationkey
      |join order on customer.c_custkey = order.o_custkey
      |join lineitem on order.o_orderkey = lineitem.l_orderkey
      |where o_orderdate > '1993-10-01'
      |and l_returnflag = 'R'
      |group by
      |n_name,
      |c_nationkey,
      |l_shipmode
      |""".stripMargin

  val Query12 =
    """
      |select
      |l_shipmode,
      |sum(o_totalprice) as sum_price,
      |avg(o_totalprice) as avg_price,
      |count(o_orderdate) as count_tuples,
      |1 + floor(rand()*100) as o_sid,
      |1 + floor(rand()*100) as l_sid
      |from order join lineitem on order.o_orderkey = lineitem.l_orderkey
      |where
      |l_receiptdate > '1994-01-01'
      |group by
      |l_shipmode
      |""".stripMargin

  val Query16 =
    """
      |select
      |p_type,
      |count(ps_suppkey) as count_supplier,
      |sum(p_retailprice) as sum_retail_price,
      |count(p_partkey) as count_tuples,
      |1 + floor(rand()*100) as ps_sid,
      |1 + floor(rand()*100) as p_sid
      |from partsupp join part on partsupp.ps_partkey = part.p_partkey
      |where
      |ps_availqty > 1000
      |and ps_availqty < 1200
      |group by
      |p_type
      |""".stripMargin

  val Query19 =
    """
      |select
      |sum(l_extendedprice*(1-l_discount)) as sum_revenue,
      |avg(l_extendedprice) as avg_price,
      |l_linenumber,
      |count(l_partkey) as count_tuples,
      |1 + floor(rand()*100) as l_sid,
      |1 + floor(rand()*100) as p_sid
      |from lineitem join part on lineitem.l_partkey = part.p_partkey
      |where
      |p_brand = 'Brand#12'
      |or
      |p_container = 'MED BAG'
      |or
      |l_quantity > 10
      |group by l_linenumber
      |""".stripMargin

  val Query21 =
    """
      |select
      |s_name,
      |avg(l_discount) as avg_disc,
      |count(l_orderkey) as count_numwait,
      |count(l_orderkey) as count_tuples,
      |1 + floor(rand()*100) as l_sid,
      |1 + floor(rand()*100) as s_sid,
      |1 + floor(rand()*100) as o_sid,
      |1 + floor(rand()*100) as n_sid
      |from
      |order join lineitem on order.o_orderkey = lineitem.l_orderkey
      |join supplier on lineitem.l_suppkey = supplier.s_suppkey
      |join nation on s_nationkey = n_nationkey
      |where
      |n_name = 'SAUDI ARABIA'
      |or n_name = 'AMERICA'
      |or n_name = 'BRAZIL'
      |group by
      |s_name
      |""".stripMargin

}
