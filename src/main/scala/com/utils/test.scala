package com.utils

import com.Tags.BusinessTag
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 测试
  * 调用两个工具类:
  * AmapUtil
  * HttpUtils
  */
object test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO 8.26
    val sqlsc = new SQLContext(sc)
    val df: DataFrame = sqlsc.read.parquet("D:\\temp")
    df.map(row => {
      // 获取商圈
      val business: List[(String, Int)] = BusinessTag.makeTags(row)
      business
    }).foreach(println)

//    val list = List("116.481488,39.990464")
//    val rdd = sc.makeRDD(list)
//    val bs: RDD[String] = rdd.map(t => {
//      val arr: Array[String] = t.split(",")
//      AmapUtil.getBusinessFromAmap(arr(0).toDouble, arr(1).toDouble)
//    })
//    bs.collect.foreach(println)

  }
}
