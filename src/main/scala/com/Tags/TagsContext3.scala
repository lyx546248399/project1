package com.Tags


import com.typesafe.config.ConfigFactory
import com.utils.TagsUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable

/**
  * 上下文标签
  * 打的所有的标签最后都需要传入这里进行汇总
  */
object TagsContext3 {
  def main(args: Array[String]): Unit = {
    // 传入的参数看需求
    if (args.length != 5) {
      println("目录不匹配,退出程序")
      sys.exit()
    }
    // 数组 接收所有的参数
    val Array(inputPath, outputPath, dirPath, stopPath,days) = args
    /**
      * inputPath: 获取文件的输入路径
      * outputPath: 文件的输出路径
      * dirPath: 字典序的传入路径
      * stopPath: 停用词库
      */
    // dirPath:D:\千峰学习内容\22\project\项目day01\Spark用户画像分析\app_dict.txt
    // stopPath:D:\千峰学习内容\22\project\项目day01\Spark用户画像分析\stopwords.txt
    // 创建上下文
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf) // 总的上下文
    val sqlcontext = new SQLContext(sc) // 里面调用的还是SparkContext


    // todo 调用Hbase API
    // 加载配置文件
    val load = ConfigFactory.load("application.conf")
    // 加载表
    val hbaseTableName: String = load.getString("hbase.TableName")
    // 创建Hadoop任务
    val configuration = sc.hadoopConfiguration
    // 加载配置(加载hbase)
    configuration.set("hbase.zookeeper.quorum", load.getString("hbase.zookeeper.quorum"))
    configuration.set("hbase.zookeeper.property.clientPort",load.getString("hbase.zookeeper.property.clientPort"))

    // 创建HbaseConnection
    val hbconn = ConnectionFactory.createConnection(configuration)
//    println(hbconn)
    // 获取操作对象, 操作对象是admin
    val hbadmin = hbconn.getAdmin
    // 判断表是否可用
    if (!hbadmin.tableExists(TableName.valueOf(hbaseTableName))) {

      // 创建表操作
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      // 创建列簇
      val descriptor = new HColumnDescriptor("tags")
      // 将列簇加入到表当中,通过admin注册这个表
      tableDescriptor.addFamily(descriptor)
      hbadmin.createTable(tableDescriptor)
      // 关闭操作顺序如下
      hbadmin.close()
      hbconn.close()
    }


    // todo 创建JobCOnf
    val jobconf = new JobConf(configuration)
    // 指定输出类型和输入到那张表 classOf强转
    jobconf.setOutputFormat(classOf[TableOutputFormat]) // 指定输出类型
    jobconf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName) // 设置表名(name,value)


    //    import sqlcontext.implicits._
    // 读取数据源
    val df: DataFrame = sqlcontext.read.parquet(inputPath)
    //    println(df.collect().toBuffer)

    // 读取字典序文件(即传入字典序未清洗前的路径)
    val map = sc.textFile(dirPath)
      .map(_.split("\t", -1))
      .filter(_.length >= 5)
      .map(arr => (arr(4), arr(1))).collect.toMap

    // 将数据进行广播
    val broadcast = sc.broadcast(map)
    // 获取停用词库
    val stopword = sc.textFile(stopPath).map((_, 0)).collect.toMap
    val bcstopword = sc.broadcast(stopword)

    // todo 8.27
    // 过滤符合Id的数据
    val baseRDD: RDD[(List[String], Row)] = df.filter(TagsUtils.OneUserId)
      .map(row => {
        // 获取所有user构建一个集合
        val userlist: List[String] = TagsUtils.getAllUserId(row)
        (userlist, row)
      })
//    baseRDD.take(10).foreach(println)

    // 构建点集合
    // flatMap: 把整体大的List拍成一个list
    val vertiesRDD: RDD[(Long, List[(String, Int)])] = baseRDD.flatMap(tp => {
      val row = tp._2
      // 所有标签
      val adList = TagsAd.makeTags(row)
      val appList = TagsAPP.makeTags(row, broadcast.value)
      val keywordList = TagsKeyword.makeTags(row, bcstopword.value)
      val dvList = TagsEquipment.makeTags(row)
      val loactionList = TagsRegion.makeTags(row)
      val business = BusinessTag.makeTags(row)
      val AllTag = adList ++ appList ++ keywordList ++ dvList ++ loactionList ++ business

      // 保证其中一个点携带着所有标签,同时也保留所有userID
      val VD: List[(String, Int)] = tp._1.map((_, 0)) ++ AllTag

      // 处理所有的点集合
      tp._1.map(uId => {
        // 保证一个点携带标签  (uid,vd),(uid,list()),(uid,list())
        if (tp._1.head.equals(uId)) {
          //
          (uId.hashCode.toLong, VD)
        } else {
          (uId.hashCode.toLong, List.empty)
        }
      })
    })
//    vertiesRDD.take(10).foreach(println)

    // 构建边的集合
    val edges = baseRDD.flatMap(tp => {
      tp._1.map(uId => Edge(tp._1.head.hashCode,uId.hashCode,0))
    })
    // 构建图
    val graph = Graph(vertiesRDD,edges)
    // 取出顶点,使用图计算中的连通图算法
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
    // 处理所有的标签和id
    vertices.join(vertiesRDD).map{
      case (uId,(conId,tagsAll)) => (conId,tagsAll)
    }.reduceByKey((list1,list2)=>{
      (list1++list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    }).take(20).foreach(println)

    sc.stop()

//    // 过滤条件,过滤符合规则的用户 --> 所有的标签都是给用户打的,所以要先过滤出用户id
//    df.filter(TagsUtils.OneUserId) // 工具类中过滤出一个不为空的用户id传过来
//      // 第一种方法
//      // 接下来所有的标签都在内部实现
//      // 下面这个map是RDD的map
//      .map(row => {
//      // 取出用户id
//      val userId = TagsUtils.getOneUserId(row) // 这是是key,以下操作取value值
//      // 接下来的通过row数据  打上 所有标签 (按照需求)
//      // 广告位类型
//      val adlist: List[(String, Int)] = TagsAd.makeTags(row)
//      // APP名称
//      val Applist: List[(String, Int)] = TagsAPP.makeTags(row, broadcast.value)
//      // 关键字标签
//      val keywordList = TagsKeyword.makeTags(row, bcstopword.value)
//      // 渠道标签
//      val platformlist: List[(String, Int)] = TagsAdplatform.makeTags(row)
//      // 设备标签
//      val equipment: List[(String, Int)] = TagsEquipment.makeTags(row)
//      // 地域标签
//      val region: List[(String, Int)] = TagsRegion.makeTags(row)
//      // 商圈标签
//      val business: List[(String, Int)] = BusinessTag.makeTags(row)
//
//      //
//      (userId, adlist ++ Applist ++ keywordList ++ equipment ++ region++business)
//      //      // App类型 使用广播变量
//      //      import sqlcontext.implicits._
//      //      val bc = sc.broadcast(appdictETL.appdictEtl(sc))
//      //      val data: Map[String, String] = bc.value
//      //      TagsAd.makeTagsApp(row,data)
//    })
//      .reduceByKey((list1, list2) =>
//        // ::: -> 把list1所有元素放入list2中
//        // List[(String, Int),(String, Int),(String, Int)]
//        (list1 ::: list2)
//          // _.1按照key进行分组 List(("APP爱奇艺",List()))
//          .groupBy(_._1)
//          .mapValues(_.foldLeft[Int](0)(_ + _._2))
//          .toList
//      )
//      // 偏函数,模式匹配中的一种
//      .map {
//      // 设置rowkey和列
//      case (userid, userTag) => {
//        // rowkey
//        val put = new Put(Bytes.toBytes(userid))
//        // 处理下标签 userTag里面是tuple类型的
//        val tags: String = userTag.map(t => t._1 + "," + t._2).mkString(",")
//        // family:Array[Byte],qualitier:Array[Byte],value:Array[Byte]
//        // 第一个参数: 标签
//        // 第二个参数: 每一天按列存的时间
//        // 第三个参数: 对应的标签值(语法糖)
////        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(s"$days"),Bytes.toBytes(tags))  // 语法糖写法
//        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(days),Bytes.toBytes(tags))
//        (new ImmutableBytesWritable(),put)  // 分别返回的key和value
//      }
//    }
//      // 存储保存到对应的表中
//      .saveAsHadoopDataset(jobconf)
//
//    sc.stop()


  }
}
