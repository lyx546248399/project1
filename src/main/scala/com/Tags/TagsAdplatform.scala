package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 获取渠道标签
  */
object TagsAdplatform extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    val platform: Int = row.getAs[Int]("adplatformproviderid")
    if(StringUtils.isNotBlank(platform.toString)){
      list:+=("CN"+platform.toString,1)
    }
    list
  }
}
