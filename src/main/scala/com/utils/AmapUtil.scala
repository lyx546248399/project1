package com.utils

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable.ListBuffer


/**
  * 商圈解析工具
  */
object AmapUtil {

  // 获取高德地图商圈信息
  def getBusinessFromAmap(long:Double,lat:Double):String={
    // https://restapi.amap.com/v3/geocode/geo?address=
    // 北京市朝阳区阜通东大街6号&output=XML&key=<用户的key>
    val location = long+","+lat
    val urlStr ="https://restapi.amap.com/v3/geocode/regeo?location=" +location +
      "&key=d250e2baaa321a58026c1d0d3c246d87"

    //https://restapi.amap.com/v3/geocode/regeo?location=116.310003,39.991957&key=<用户的key>&radius=1000&extensions=all
    // 调用请求
    val jsonstr = HttpUtils.get(urlStr)
//    return jsonstr
    // 解析json串
    val jsonparse = JSON.parseObject(jsonstr)
    // 判断状态是否成功
    // 0 表示请求失败；1 表示请求成功
    val status: Int = jsonparse.getIntValue("status")
    // 如果为空,返回空串,一下代码就不继续进行了
    if(status == 0) return "status=0"
    // 接下来解析内部json串,判断每个key的value都不能为空
    val regeocodeJson: JSONObject = jsonparse.getJSONObject("regeocode")
    if(regeocodeJson == null || regeocodeJson.keySet().isEmpty) return  "regeocode isEmpty"

    val addressComponentJson: JSONObject = regeocodeJson.getJSONObject("addressComponent")
    if(addressComponentJson == null || addressComponentJson.keySet().isEmpty) return "addressComponent"

    val businessAreasArray: JSONArray = addressComponentJson.getJSONArray("businessAreas")
    if(businessAreasArray == null || businessAreasArray.isEmpty) return null
    // 创建集合 保存数据
    val buffer: ListBuffer[String] = collection.mutable.ListBuffer[String]()
    // 循环输出
    for(item <- businessAreasArray.toArray){
      if(item.isInstanceOf[JSONObject]){
        val json: JSONObject = item.asInstanceOf[JSONObject]
        buffer.append(json.getString("name"))
      }
    }
    // 每一个商圈返回回来的时候加上一个","
    buffer.mkString(",")

  }
}
