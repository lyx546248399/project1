package com.MediaAnalysis

import java.util.Properties

import com.ETL.appdictETL
import com.Rpt.equipmentRpt.isType
import com.utils.RptUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * 媒体分析
  * 广播变量
  */
object mediaAnalysis {
  def main(args: Array[String]): Unit = {
    // 初始化
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlcontext = new SQLContext(sc)
    // 获取数据
    val df: DataFrame = sqlcontext.read.parquet("D:\\temp")

    // 2. 将数据进行处理,统计各个指标  (媒体类别,九个指标))

    val operators= df.map(row => {
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
      val appname: String = row.getAs[String]("appname")
      val appid: String = row.getAs[String]("appid")
      // 从RptUtils工具类拿到九个基础指标
      val reqlist: List[Double] = RptUtils.request(requestmode, processnode)
      val clicklist: List[Double] = RptUtils.click(requestmode, iseffective)
      val adlist: List[Double] = RptUtils.Ad(iseffective, isbilling, isbid, iswin,
        adorderid, winprice, adpayment)

      // 返回一个元组  形式:(媒体类别,九个指标)
      ((appname,appid), reqlist ++ clicklist ++ adlist)
    })

    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "123456")
    val url = "jdbc:mysql://localhost:3306/bigdata"
    // 竞价成功率	竞价成功率=竞价成功数/参与竞价数
    // 点击率	点击率=点击量/展示量
    import sqlcontext.implicits._
    val bc = sc.broadcast(appdictETL.appdictEtl(sc))
    val res: DataFrame = operators.reduceByKey((list1, list2) => {
      list1.zip(list2).map(ele => ele._1 + ele._2)
    }).map(x => ({
      val data = bc.value
       if(x._1._1 == null){
         data.getOrElse(x._1._2,"0")  // getOrElse这个方法是通过传递参数(key)获取另外一个表中对应的Map键值对中的value值
       }else{
         x._1._1
       }
    }
      , x._2(0), x._2(1), x._2(2), x._2(3), x._2(4),
      if (x._2(3) == 0) 0 else x._2(4) / x._2(3), x._2(5), x._2(6), if (x._2(5) == 0) 0 else x._2(6) / x._2(5),
      x._2(7), x._2(8)))
      .toDF("媒体类别", "总请求", "有效请求", "广告请求", "参与竞价数", "竞价成功数",
        "竞价成功率", "展示量", "点击量", "点击率", "广告消费", "广告成本")
    res.show()
    res.write.jdbc(url, "meta", connectionProperties)
    sc.stop()
  }
  //
//  def appnameAndId(appname:String,appid:String) :String = {
//    if(appname == null){
//
//    }
//  }
}
