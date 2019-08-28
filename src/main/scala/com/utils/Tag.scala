package com.utils

/**
  * 打标签的统一接口
  */
trait Tag {
  // Any* 代表传多个参数 不能保证传的参数是Row类型的,所以用Any
  // List[(String,Int)]: 这个集合代表所有的标签
  // String,Int --> 代表标签的key(标签名字)--value(标签值)
  def makeTags(args:Any*):List[(String,Int)]  // 抽象方法
}
