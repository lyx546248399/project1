package com.Rpt

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 地域分布指标
  * 用SQL实现
  */
object LocationRptSQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName)
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val sqlcontext = new SQLContext(sc)

    // 读取二进制文件
    val df: DataFrame = sqlcontext.read.parquet("D:\\temp")
    // 将读取的数据存储在一个临时表中

    df.registerTempTable("temp")

    // 为了解决每行的空格，引入了stripMargin方法。
    // 使用方式是把管道符号（|）放在每行前面，然后对整个字符串调用该方法。
    val res = sqlcontext.sql(
      """
        |select
        |provincename,
        |cityname,
        |sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) originalrequest,
        |sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) validrequest,
        |sum(case when requestmode = 1 and processnode >= 3 then 1 else 0 end) Advertisingrequest,
        |sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) Bidparticipation,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) Successfulbid,
        |sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) shownums,
        |sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) clicknums,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice/1000 else 0 end) adspend,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment/1000 else 0 end) adcost
        |from temp
        |group by provincename,cityname
      """.stripMargin)

    res.show()

    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","123456")
    val url = "jdbc:mysql://localhost:3306/bigdata"
    res.write.jdbc(url,"procitysql",prop)

    // 存储数据到磁盘
//    res.coalesce(1).write.partitionBy("provincename","cityname").json("D:\\procity_sql")

    sc.stop()
  }
}
