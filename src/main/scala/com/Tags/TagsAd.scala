package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row


/**
  * 广告位类型标签
  * 打标签的统一接口
  */
object TagsAd extends Tag{
  /**
    * 广告类
    * @param args
    * @return
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    // 创建一个可变的list集合
    var list = List[(String,Int)]()

    // 解析参数 将row传进来
    // args(0)--> 0代表取出的第一个参数
    val row = args(0).asInstanceOf[Row]   //强转
    // 获取广告类型Int,广告类型名称String
    val adType: Int = row.getAs[Int]("adspacetype")
    adType match {
      case v if v>9 => list:+=("LC" + v,1)
      case v if v<=9 && v>0 => list:+=("lC0" +v,1)
    }
    // 广告的名字
    val adName: String = row.getAs[String]("adspacetypename")
    //  判断是否为空,代码的完整性
    if(StringUtils.isNotBlank(adName)){
      list:+=("LN"+adName,1)
    }
    list
  }

  /**
    * 获取APP名称
    * @return
    */
//  override def makeTagsApp(args: Any*): List[(String, Int)] = {
//    var list = List[(String,Int)]()
//    val row: Row = args(0).asInstanceOf[Row]
//    // 广播过来的字典序
//    val app_dict: Map[String, String] = args(1).asInstanceOf[Map[String, String]]
//    // 获取APP的名称,通过广播变量与APPID进行匹配获得appName
//    val appName: String = row.getAs[String]("appname")
//    val appId: String = row.getAs[String]("appid")
//    if(StringUtils.isNotBlank(appName)){
//      list:+=("APP"+appName,1)
//    }else{
//      list:+=("APP"+app_dict.getOrElse(appId,"0"),1)
//    }
//    list
//  }

  /**
    * 渠道标签
    * @return
    */
//  override def makeTagsChannel(args: Any*): List[(String, Int)] = {
//    var list = List[(String,Int)]()
//    val row: Row = args(0).asInstanceOf[Row]
//    val platTags: Int = row.getAs[Int]("adplatformproviderid")
//    if(StringUtils.isNotBlank(platTags.toString)){
//      list:+=("CN"+platTags.toString,1)
//    }
//    list
//  }
}
