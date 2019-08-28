package com.Tags

import ch.hsr.geohash.GeoHash
import com.utils.{AmapUtil, JedisConnectionPool, Tag, Utils2Type}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

// TODO 8.26
/**
  * 商圈标签
  * 第一次访问高德或百度获取商圈存入数据库,
  * 把商圈做缓存-->节省成本
  * 实现步骤:
  * 1、去nosql数据库查询商圈信息(如果存在,拿取;不存在,去高德获取商圈)
  * 2、获取的商圈信息在存入nosql当中
  */
object BusinessTag extends Tag {
  /**
    * 打标签的统一接口
    *
    * @param args
    * @return
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    // 解析参数
    val row: Row = args(0).asInstanceOf[Row]
    // 获取经度
    val long = row.getAs[String]("long")
    // 获取纬度
    val lat = row.getAs[String]("lat")

    // 获取经纬度(保证经纬度是中国范围内的1)并过滤
    //中国的经纬度范围大约为：纬度3.86~53.55，经度73.66~135.05, 不在范围类的数据可需要处理.
    if (Utils2Type.toDouble(long) >= 73.0 && Utils2Type.toDouble(long) <= 135.0 &&
      Utils2Type.toDouble(lat) >= 3.0 && Utils2Type.toDouble(lat) <= 54.0) {
      //先去数据库获取商圈(方法)
      val busniess: String = getBusiness(long.toDouble,lat.toDouble)
      //判断缓存中是否有此商圈
      if (StringUtils.isNotBlank(busniess)) {
        val lines = busniess.split(",")
        lines.foreach(f => list:+=(f,1))
      }
    }
      list
  }

  /**
    * 2、获取数据库商圈信息
    * 步骤: 先去数据库查询此商圈是否存在,如果不存在,调用高德地图解析商圈,最后存入redis中
    * 返回的是商圈
    */
  def getBusiness(long: Double, lat: Double): String = {
    // 转换GeoHash字符串-->获取key "8"代表编码长度
    // 获取的geohash就是key
    val geohash: String = GeoHash.geoHashStringWithCharacterPrecision(lat,long, 8)
    // 1、去数据库查询此商圈是否存在(不管是否存在,返回一个商圈信息,在以下步骤在进行判断)
    val business: String = redis_queryBusniess(geohash)
    // 2、判断商圈是否为空
    if (business == null || business.length == 0) {
      // 通过经纬度获取商圈
      val business: String = AmapUtil.getBusinessFromAmap(long.toDouble, lat.toDouble)
      // 3、如果调用高德地图解析商圈,那么需要将此次商圈的信息存入Redis
      redis_insertBusniess(geohash, business)
    }
    business
  }

  /**
    * 1、去数据库获取商圈信息
    */
  def redis_queryBusniess(geoHash: String): String = {
    val jedis: Jedis = JedisConnectionPool.getConnection()
    // 通过Jedis连接redis,查询geoHash是否存在在此redis中
    val business: String = jedis.get(geoHash)
    jedis.close()
    business
  }

  /**
    * 2.1、存储商圈到redis
    */

  def redis_insertBusniess(geoHash: String, business: String) = {
    val jedis: Jedis = JedisConnectionPool.getConnection()
    val setbusiness: String = jedis.set(geoHash, business)
    jedis.close()

  }

}