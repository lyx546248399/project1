package examFirst

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer

object TestExam {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val jsonData: RDD[String] = sc.textFile("D:\\千峰学习内容\\22\\project\\第一周考试\\json.txt")

    val res: RDD[ListBuffer[(String, String)]] = jsonData.map(line => {

      // 缓存集合
      var list = ListBuffer[(String, String)]()
      // 解析json字符串
      val jsonparse: JSONObject = JSON.parseObject(line)
      // 判断status状态
      val status: Int = jsonparse.getIntValue("status")
      if (status == 0) "status=0"
      else {
        val regeocodeJson: JSONObject = jsonparse.getJSONObject("regeocode")
        if (regeocodeJson == null || regeocodeJson.keySet().isEmpty) ""
        else {
          val poisArr: JSONArray = regeocodeJson.getJSONArray("pois")
          if (poisArr == null || poisArr.isEmpty) ""
          else {
            for (i <- 0 until poisArr.size()) {
              val poi: JSONObject = poisArr.getJSONObject(i)
              //                val poiId: String = poi.get("id").toString
              val busine: String = poi.get("businessarea").toString
              val poitype: String = poi.get("type").toString
              // 返回的是businessarea   type
              list.append((busine, poitype))
            }
          }
        }
      }
      list

    })
//    res.map(t => t.foreach(println)).collect()
//    jsonData.map(println(_))
//    jsonData.foreach(println)


    // 按照pois，分类businessarea，并统计每个businessarea的总数。
    //      res
    //      .map(line => (line,1))
    //      .filter(_._1 !="[]")
    //      .collect()
    //      .foreach(println)
    //      .reduceByKey(_+_)

//        res.map(list => {
//          list.map(x => (x._1,1)).toMap.groupBy(_._1).mapValues(_.size).foreach(println)
//        })
    res.map(
      _.map(x => (x._1, 1)).filter(_._1 != "[]").groupBy(_._1).mapValues(_.size))
      .foreach(println)

    //2、按照pois，分类type，为每一个Type类型打上标签，统计各标签的数量
    // ListBuffer[(String, String)]
    res.map(_.map(x =>
      (x._2.split(";")
        .map((_, 1)))).flatten.
      groupBy(_._1)
      .mapValues(_.size)).foreach(println)
  }
}
