package examFirst

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWord {
  def main(args: Array[String]): Unit = {
    //    val lines = List("hello tom hello jerry", "hello suke hello", "hello tom")
    //
    //    val stringToStrings =
    //      lines.flatMap(_.split(" ")).groupBy(x => x)
    //        //Map(tom -> List(tom, tom), jerry -> List(jerry)
    //        .mapValues(x => x.size)
    //        // Map(tom -> 2, jerry -> 1, hello -> 5, suke -> 1)
    //          .toList
    //    // List((tom,2), (jerry,1), (hello,5), (suke,1))
    //          .sortBy(_._2).reverse
    //    println(stringToStrings)
    val conf: SparkConf = new SparkConf()
    conf.setAppName("sparkwordcount")
    conf.setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val lines= sc.textFile("D:\\千峰学习内容\\txtt.txt")

    // 对数据做切分，生成一个个单词
    val words: RDD[String] = lines.flatMap(_.split(" "))
    println(words.collect.toBuffer)
    // 将单词生成一个元组->(word, 1), 便于计数统计
    val tup: RDD[(String, Int)] = words.map((_, 1))
    println(tup.collect.toBuffer)
    // 开始聚合统计
    val sumed: RDD[(String, Int)] = tup.reduceByKey(_+_)
    println(sumed.collect.toBuffer)
    // 降序排序
    val sorted: RDD[(String, Int)] = sumed.sortBy(_._2, false)
    println(sorted.collect.toBuffer)
    // 结果输出
    //    sorted.collect.foreach(println) // foreach是Scala的方法
    //    sorted.foreach(println) // foreach是Spark提供的api
//    println(sorted.collect.toBuffer)

    // 存储到HDFS
    //    sorted.saveAsTextFile("hdfs://node01:9000/out-20190729-1")
    //    sorted.saveAsTextFile(args(1))

    // 释放sc资源
    sc.stop()
  }
}
