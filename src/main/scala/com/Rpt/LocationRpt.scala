package com.Rpt

import java.util.Properties

import com.utils.RptUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 指标一:
  * 地域分布指标
  * 分别写入到Mysql中和磁盘上
  */
object LocationRpt {
  def main(args: Array[String]): Unit = {
    // 1 判断路径是否正确
    if(args.length != 1){
      println("目录参数不正确,退出程序")
      sys.exit()
    }

    // 创建一个集合保存输出和输出列表
    val Array(inputPath) = args
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式  采用Kryo序列化方式,比默认序列化方式性能高
      // 默认情况，Spark使用Java自带的ObjectOutputStream 框架来序列化对象
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // 创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    /**
      * Spark core方式
      */

    // 1. 获取数据
    val df = sQLContext.read.parquet(inputPath)
    // 2. 将数据进行处理,统计各个指标  ((省,市),指标))
    val tupProCityTarget: RDD[((String, String), List[Double])] = df.map(row => {
      // 把需要的字段全部取出
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")

      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")

      // key值  是地域的省市
      val pro: String = row.getAs[String]("provincename")
      val city: String = row.getAs[String]("cityname")

      // 创建三个对应的方法处理九个指标

      val reqlist: List[Double] = RptUtils.request(requestmode, processnode)
      val clicklist: List[Double] = RptUtils.click(requestmode, iseffective)
      val adlist: List[Double] = RptUtils.Ad(iseffective, isbilling, isbid, iswin,
        adorderid, winprice, adpayment)

      // 返回一个元组  形式:((省,市),九个指标)
      ((pro, city), reqlist ++ clicklist ++ adlist)
    })

    import sQLContext.implicits._

    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "123456")
    val url = "jdbc:mysql://localhost:3306/bigdata"

    // 根据key聚合Value
    // tupProCityTarget: ((String, String), List[Double])
    tupProCityTarget.reduceByKey((list1, list2) => {
      // 将List中的数据通过zip拉在一起
      // list((1,1),(2,2),(3,3))
      //      t => t._1 + t._2  --> List(2, 4, 6, 8)
      list1.zip(list2).map(t => t._1 + t._2)
    })
      .map(x => (x._1.toString(),x._2(0).toDouble ,x._2(1), x._2(2), x._2(3), x._2(4),if(x._2(3) == 0) 0 else x._2(4)/x._2(3),
        x._2(5), x._2(6), if(x._2(5) == 0) 0 else x._2(6)/x._2(5), x._2(8),x._2(7)))
      .toDF("省市城市","原始请求","有效请求", "广告请求", "参与竞价数", "竞价成功数", "竞价成功率","展示量", "点击量", "点击率", "广告成本","广告消费")

      //      .show()
      .write.jdbc(url,"region",connectionProperties)

      //      // 存入Mysql中,使用foreachPartition
      //      .foreachPartition(x => x.toIterator)

      //存到磁盘上
      /*
      .map(t => {
      // 整理元素
      t._1 + "," + t._2.mkString(",")
    })
    */

//      res1.saveAsTextFile(outputPath)
//    val connectionProperties = new Properties()
//    connectionProperties.put("user", "root")
//    connectionProperties.put("password", "123456")
//    //表可以不存在,通过读取的数据可以直接生成表
//    res1.write.jdbc("jdbc:mysql://localhost:3306/bigdata","region",connectionProperties)

    sc.stop()
  }
}
