package com.ProCityCount

/**
  * 将数据导入到Mysql中
  */

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将数据读取并存入Mysql中
  * 方式一: 小斌版
  */
object procityMysql {
  def main(args: Array[String]): Unit = {
    // 1 判断路径是否正确
    if(args.length != 1){
      println("目录参数不正确,退出程序")
      sys.exit()
    }

    // 创建一个集合保存输出和输出列表
    val Array(inputPath) = args
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getName).setMaster("local[*]")
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

    val res: RDD[((String, String), Int)] = df.select("provincename", "cityname")
      .rdd
      .map(fields => (fields.getString(0), fields.getString(1)))
      .map((_, 1))
      .reduceByKey(_ + _)
      // false
      .sortBy(_._2, false)
    res.collect().toList.foreach(println)

    val scheduma = StructType(
      List(
        StructField("provincename",StringType),
        StructField("cityname",StringType),
        StructField("count",IntegerType)
      )
    )

    val resDF: DataFrame = sQLContext.createDataFrame(res.map(x => Row(x._1._1,x._1._2,x._2)),scheduma)

    // 获取数据库连接
    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","123456")
    val url = "jdbc:mysql://localhost:3306/bigdata"

    // 存储到mysql中
    resDF.write.jdbc(url,"cou",prop)

    sc.stop()

  }
}
