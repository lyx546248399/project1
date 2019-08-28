package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

/**
  * 设备标签
  */
object TagsEquipment extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    val row = args(0).asInstanceOf[Row]
    val eqclient: Int = row.getAs[Int]("client")
    val network: String = row.getAs[String]("networkmannername")
    val ispname: String = row.getAs[String]("ispname")
    // 设备操作系统
    eqclient match {
      case 1 => list:+=("D00010001",1)
      case 2 => list:+=("D00010002",1)
      case 3 => list:+=("D00010003",1)
      case _ => list:+=("D00010004",1)
    }
    // 设备联网方式
    network match{
      case v if v.equals("WIFI") => list:+=("D00020001",1)
      case v if v.equals("4G") => list:+=("D00020002",1)
      case v if v.equals("3G") => list:+=("D00020003",1)
      case v if v.equals("2G") => list:+=("D00020004",1)
      case _ => list:+=("D00020005",1)
    }
    // 设备运营商方式
    ispname match {
      case v if v.equals("移动") => list:+=("D00030001",1)
      case v if v.equals("联通") => list:+=("D00030001",1)
      case v if v.equals("电信") => list:+=("D00030001",1)
      case _ => list:+=("D00030001",1)
    }
    list

  }
}
