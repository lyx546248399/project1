package GraphX

import java.util.Properties

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object graph_vip827 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlcontext = new SQLContext(sc)

    // 构造点集合
    val vertexRDD: RDD[(Long, (String, Int))] = sc.makeRDD(Seq(
      (1L, ("小红", 20)),
      (3L, ("小明", 33)),
      (5L, ("小七", 20)),
      (7L, ("小王", 60)),
      (9L, ("小李", 20)),
      (11L, ("小美", 30))
    ))
    // 构造边集合
    val edges: RDD[Edge[Int]] = sc.makeRDD(Seq(
      Edge(1L, 3L, 0),
      Edge(3L, 9L, 0),
      Edge(9L, 11L, 0),
      Edge(1L, 3L, 0),
      Edge(5L, 7L, 0)
    ))
    // 构造图
    val graph = Graph(vertexRDD,edges)
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
    val res = vertices.join(vertexRDD).map {
      case (userId, (conId, (name, age))) => {
        (conId, List(name, age))
      }
    }.reduceByKey(_ ++ _)
    res.foreach(println)

    val res2 = res.map(x => {
      val userId: Int = x._1.toInt
      val rel = x._2 + ""
      (userId, rel)
    })

    val rd = res2.map((x => (x._1,x._2)))
    import sqlcontext.implicits._
    val df: DataFrame = rd.toDF("userId","rel")
    df.registerTempTable("graph_bb")
    val res3: DataFrame = sqlcontext.sql("select userId,rel from graph_bb group by userId,rel")

    // 加载数据到Mysql上
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "123456")
    //表可以不存在,通过读取的数据可以直接生成表
    res3.write.jdbc("jdbc:mysql://localhost:3306/bigdata","graph01",connectionProperties)

  }
}
