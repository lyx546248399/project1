package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * 获取APP类型标签
  */
object TagsAPP extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    // 广播过来的字典序数据
    val appmap= args(1).asInstanceOf[Map[String, String]]
    // 获取appname和appid
    val appname: String = row.getAs[String]("appname")
    val appid: String = row.getAs[String]("appid")
    // 进行空值判断
    if(StringUtils.isNotBlank(appname)){
      list:+=("APP"+appname,1)
    }else if(StringUtils.isNotBlank(appid)){
      list:+=("APP"+appmap.getOrElse(appid,"0"),1)
    }
    list
  }

}
