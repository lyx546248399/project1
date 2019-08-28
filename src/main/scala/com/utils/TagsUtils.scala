package com.utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 标签工具类
  * 作用:取出一个不为空的用户id
  */
object TagsUtils {

  // 过滤我们所需要的字段 需求:只要下面字段中有一个不为空的取出来即可
  // 取出的字段表明用户的id(用户的id可能是手机、平板等登陆,)
  val OneUserId =
    """
      |imei != '' or mac != '' or openudid != '' or androidid != '' or idfa != '' or
      |imeimd5 != '' or macmd5 != '' or openudidmd5 != '' or androididmd5 != '' or idfamd5 != '' or
      |imeisha1 != '' or macsha1 != '' or openudidsha1 != '' or androididsha1 != '' or idfasha1 != ''
      |
    """.stripMargin
  // 取出唯一不为空的id
  def getOneUserId(row:Row):String ={
    // 使用模式匹配
    row match {
      case v if StringUtils.isNotBlank(v.getAs[String]("imei")) => "IM:" + v.getAs[String]("imei")
      case v if StringUtils.isNotBlank(v.getAs[String]("mac")) => "MAC:" + v.getAs[String]("mac")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudid")) => "OPEN:" + v.getAs[String]("openudid")
      case v if StringUtils.isNotBlank(v.getAs[String]("androidid")) => "ANDROID:" + v.getAs[String]("androidid")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfa")) => "IDFA:" + v.getAs[String]("idfa")

      case v if StringUtils.isNotBlank(v.getAs[String]("imeimd5")) => "IM5:" + v.getAs[String]("imeimd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("macmd5")) => "MAC5:" + v.getAs[String]("macmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudidmd5")) => "OPEN5:" + v.getAs[String]("openudidmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("androididmd5")) => "ANDROID5:" + v.getAs[String]("androididmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfamd5")) => "IDFA5:" + v.getAs[String]("idfamd5")

      case v if StringUtils.isNotBlank(v.getAs[String]("imeisha1")) => "IMS1:" + v.getAs[String]("imeisha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("macsha1")) => "MACS1:" + v.getAs[String]("macsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudidsha1")) => "OPENS1:" + v.getAs[String]("openudidsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("androididsha1")) => "ANDROIDS1:" + v.getAs[String]("androididsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfasha1")) => "IDFAS1:" + v.getAs[String]("idfasha1")
    }
  }

  // 获取所有Id
  def getAllUserId(row:Row):List[String] ={
    var list = List[String]()
    if (StringUtils.isNotBlank(row.getAs[String]("imei")))  list:+="IM:" + row.getAs[String]("imei")
    if (StringUtils.isNotBlank(row.getAs[String]("mac")))  list:+="MAC:" + row.getAs[String]("mac")
    if (StringUtils.isNotBlank(row.getAs[String]("openudid")))  list:+="OPEN:" + row.getAs[String]("openudid")
    if (StringUtils.isNotBlank(row.getAs[String]("androidid")))  list:+="ANDROID:" + row.getAs[String]("androidid")
    if (StringUtils.isNotBlank(row.getAs[String]("idfa")))  list:+="IDFA:" + row.getAs[String]("idfa")
    if (StringUtils.isNotBlank(row.getAs[String]("imeimd5")))  list:+="IM5:" + row.getAs[String]("imeimd5")
    if (StringUtils.isNotBlank(row.getAs[String]("macmd5")))  list:+="MAC5:" + row.getAs[String]("macmd5")
    if (StringUtils.isNotBlank(row.getAs[String]("openudidmd5")))  list:+="OPEN5:" + row.getAs[String]("openudidmd5")
    if (StringUtils.isNotBlank(row.getAs[String]("androididmd5")))  list:+="ANDROID5:" + row.getAs[String]("androididmd5")
    if (StringUtils.isNotBlank(row.getAs[String]("idfamd5")))  list:+="IDFA5:" + row.getAs[String]("idfamd5")
    if (StringUtils.isNotBlank(row.getAs[String]("imeisha1")))  list:+="IMS1:" + row.getAs[String]("imeisha1")
    if (StringUtils.isNotBlank(row.getAs[String]("macsha1")))  list:+="MACS1:" + row.getAs[String]("macsha1")
    if (StringUtils.isNotBlank(row.getAs[String]("openudidsha1")))  list:+="OPENS1:" + row.getAs[String]("openudidsha1")
    if (StringUtils.isNotBlank(row.getAs[String]("androididsha1")))  list:+="ANDROIDS1:" + row.getAs[String]("androididsha1")
    if (StringUtils.isNotBlank(row.getAs[String]("idfasha1")))  list:+="IDFAS1:" + row.getAs[String]("idfasha1")

    list
  }

}
