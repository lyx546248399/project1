package com.ETL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext}

/**
  * 字典文件清洗
  */
object appdictETL {
  def appdictEtl(sc:SparkContext) = {

    val dataRDD: RDD[Array[String]] = sc.textFile("D:\\千峰学习内容\\22\\project\\项目day01\\Spark用户画像分析\\app_dict.txt")
      .map(lines => {
        // "\\s"是去除所有的Tab和空格
        lines.split("\\s")
      }).filter(_.length >= 5)     // 过滤数据,如果数据小于5条就是没有用的

    val tupRDD: RDD[(String, String)] = dataRDD.map(x => {
      (x(4), x(1))
    })
    // 广播变量时不能使用RDD,将RDD转换为一个ARRAY集合
    val res: Array[(String, String)] = tupRDD.collect()
    //    tupRDD.saveAsTextFile("D:\\app_dict")
    res.toMap
  }
}
