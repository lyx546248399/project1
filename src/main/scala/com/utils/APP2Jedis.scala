package com.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object APP2Jedis {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = new SparkContext(conf)
    // 读取字段文件
    val dict: RDD[String] = sc.textFile("D:\\千峰学习内容\\22\\project\\项目day01\\Spark用户画像分析\\app_dict.txt")
    // 处理字段文件数据
    dict.map(_.split("\t", -1))
      .filter(_.length >= 5)
      .foreachPartition( arr => {
        // 拿到Jedis的连接池
        val jedis: Jedis = JedisConnectionPool.getConnection()
        arr.foreach(arr => {
          jedis.set(arr(4),arr(1))
        })
        jedis.close()
      }
      )

  }
}
