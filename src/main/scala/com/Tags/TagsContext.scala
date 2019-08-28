package com.Tags

import com.utils.{JedisConnectionPool, TagsUtils}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object TagsContext {
  def main(args: Array[String]): Unit = {
    if(args != 4){
      println("目录不匹配，退出程序")
      sys.exit()
    }

    val Array(inputPath,outputPath,dirPath,stopPath)=args
    // 创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 读取数据
    val df = sQLContext.read.parquet(inputPath)
    // 读取字段文件(字典序)
    val map = sc.textFile(dirPath).map(_.split("\t",-1))
      .filter(_.length>=5).map(arr=>(arr(4),arr(1))).collectAsMap()

    // todo 将处理好的数据进行广播
    val broadcast = sc.broadcast(map)
    // 获取停用词库
    val stopword = sc.textFile(stopPath).map((_,0)).collect.toMap
    val bcstopword = sc.broadcast(stopword)

    // 过滤符合Id的数据
    df.filter(TagsUtils.OneUserId)
      // 所有标签在内部实现
      .mapPartitions(row => {
      // todo Jedis连接Redis
      val jedis: Jedis = JedisConnectionPool.getConnection()
      var list = List[(String,List[(String,Int)])]()

      row.map(row => {
        // 取出用户Id
      val userId: String = TagsUtils.getOneUserId(row)
      // 通过row数据,打上所有标签(按照需求)
      val adlist: List[(String, Int)] = TagsAd.makeTags(row)
      val applist: List[(String, Int)] = TagsAPP.makeTags(row,jedis)
      val keywordlist: List[(String, Int)] = TagsKeyword.makeTags(row,bcstopword.value)
      val eqlist: List[(String, Int)] = TagsEquipment.makeTags(row)
      val regionlist: List[(String, Int)] = TagsRegion.makeTags(row)

      list:+=(userId,adlist++applist++keywordlist++eqlist++regionlist)

    })
      // 关闭Jedis
      jedis.close()
      list.iterator
    })
      .reduceByKey((list1,list2) =>
        (list1:::list2)
        .groupBy(_._1)
        .mapValues(_.foldLeft[Int](0)(_ + _._2))
        .toList
      ).foreach(println)

  }
}
