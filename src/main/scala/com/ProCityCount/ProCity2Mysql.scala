package com.ProCityCount

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 从本地磁盘加载数据到Mysql上
  * 方式二
  */
object ProCity2Mysql {
  def main(args: Array[String]): Unit = {
    // 1 判断路径是否正确
//    if(args.length != 2){
    if(args.length != 1){
      println("目录参数不正确,退出程序" + args.length)
      sys.exit()
    }
    // 创建一个集合保存输出和输出列表
//    val Array(inputPath,outputPath) = args
    val Array(inputPath) = args
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式  采用Kryo序列化方式,比默认序列化方式性能高
      // 默认情况，Spark使用Java自带的ObjectOutputStream 框架来序列化对象
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // 创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 设置压缩方式 使用Snappy方式进行压缩--> 优点:压缩的速率
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")

    // 读取本地文件
    val df: DataFrame = sQLContext.read.parquet(inputPath)
    // 将读取的数据注册成临时表
    df.registerTempTable("log")
    // 指标统计
    val res: DataFrame = sQLContext.sql("select count(*) ct,provincename,cityname from log group by provincename,cityname")

    // 1、存到磁盘上
//            res.write.json(outputpath)
    // 减少分区
//        res.coalesce(1).write.partitionBy("provincename","cityname").json(outputPath)

    // 加载数据到Mysql上
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "123456")
    //表可以不存在,通过读取的数据可以直接生成表
    res.write.jdbc("jdbc:mysql://localhost:3306/bigdata","ProCity",connectionProperties)

    sc.stop()


  }
}
