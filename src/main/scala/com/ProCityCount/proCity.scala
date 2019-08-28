package com.ProCityCount

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求指标二:
  * 统计地域、各省市分布情况
  */
object proCity {
  def main(args: Array[String]): Unit = {

    // 1 判断路径是否正确
    if(args.length != 2){
      println("目录参数不正确,退出程序")
      sys.exit()
    }

    // 创建一个集合保存输出和输出列表
//    val Array(inputPath,outputpath) = args
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

    // 注册临时表
    df.registerTempTable("log")
    // 指标统计
    val res: DataFrame = sQLContext.sql("select count(*) ct,provincename,cityname from log group by provincename,cityname")

    // 1、存到磁盘上
//        res.write.json(outputPath)
    // 减少分区
//    res.coalesce(1).write.partitionBy("provincename","cityname").json(outputPath)


    /*
    // 加载配置文件, 需要使用对应的依赖
    val load = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("uesr",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    res.write.mode("append").jdbc(load.getString("jdbc.url"),load.getString("jdbc.TabName"),prop)
    */


    sc.stop()


  }

  // 请求数据库的配置信息
//  def getProperties() = {
//    val prop = new Properties()
//    prop.put("user","root")
//    prop.put("password","123456")
//    val url = "jdbc:mysql//localhost:3306/local"
//    (prop,url)
//  }
}
