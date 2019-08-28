package com.Rpt

import java.util.Properties

import com.Rpt.equipmentRpt.isType
import com.utils.RptUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * 操作系统
  */
object operateSysRpt {
  def main(args: Array[String]): Unit = {
    // 初始化
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlcontext = new SQLContext(sc)
    // 获取数据
    val df: DataFrame = sqlcontext.read.parquet("D:\\temp")

    // 2. 将数据进行处理,统计各个指标  (操作类,九个指标))

    val operators: RDD[(String, List[Double])] = df.map(row => {
      // 把需要的九个指标全部取出
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")

      // 设备类型
      val client: Integer = row.getAs[Integer]("client")
      // 从RptUtils工具类拿到九个基础指标
      val reqlist: List[Double] = RptUtils.request(requestmode, processnode)
      val clicklist: List[Double] = RptUtils.click(requestmode, iseffective)
      val adlist: List[Double] = RptUtils.Ad(iseffective, isbilling, isbid, iswin,
        adorderid, winprice, adpayment)

      // 返回一个元组  形式:(设备类型,九个指标)
      (isEquipmentType(client), reqlist ++ clicklist ++ adlist)
    })
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "123456")
    val url = "jdbc:mysql://localhost:3306/bigdata"
    // 竞价成功率	竞价成功率=竞价成功数/参与竞价数
    // 点击率	点击率=点击量/展示量
    import sqlcontext.implicits._
    val res: DataFrame = operators.reduceByKey((list1, list2) => {
      list1.zip(list2).map(ele => ele._1 + ele._2)
    }).map(x => (x._1.toString, x._2(0), x._2(1), x._2(2), x._2(3), x._2(4),
      if (x._2(3) == 0) 0 else x._2(4) / x._2(3), x._2(5), x._2(6), if (x._2(5) == 0) 0 else x._2(6) / x._2(5),
      x._2(7), x._2(8)))
      .toDF("操作类", "总请求", "有效请求", "广告请求", "参与竞价数", "竞价成功数",
        "竞价成功率", "展示量", "点击量", "点击率", "广告消费", "广告成本")
    res.show()
    res.write.jdbc(url, "client", connectionProperties)

    sc.stop()
  }

  def isEquipmentType(client:Integer) :String ={
    if(client == 1){
      "android "
    }else if(client == 2){
      "ios"
    }else if(client == 3){
      "wp"
    }else{
      "其他"
    }
  }
}
