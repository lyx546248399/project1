package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 地域标签
  */
object TagsRegion extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    // 省标签
    val pn: String = row.getAs[String]("provincename")
    // 市标签
    val cn: String = row.getAs[String]("cityname")

    if(StringUtils.isNotBlank(pn)){
      list:+=("ZP"+pn,1)
    }
    if(StringUtils.isNotBlank(cn)){
      list:+=("ZC"+cn,1)
    }

    list
  }
}
