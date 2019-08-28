package com.utils

/**
  *指标方法
  * 依据Execl中计算逻辑处理
  */
object RptUtils {

  // 此方法处理请求数

  def request(requsetmode:Int,processnode:Int):List[Double] = {
    // 三个指标: 原始请求数,有效请求数,广告请求数
    if(requsetmode == 1 && processnode == 1){
      List[Double](1,0,0)
    }else if(requsetmode == 1 && processnode == 2){
      List[Double](1,1,0)
    }else if(requsetmode == 1 && processnode == 3){
      List[Double](1,1,1)
    }else{
      List[Double](0,0,0)
    }
  }

  // 此方法处理展示点击数

  def click(requsetmode:Int,iseffective:Int):List[Double] = {
    // 两个指标: 展示数,点击数
    if(requsetmode == 2 && iseffective == 1){
      List[Double](1,0)
    }else if(requsetmode == 3 && iseffective == 1){
      List[Double](0,1)
    }else{
      List[Double](0,0)
    }
  }

  // 此方法处理竞价操作

  def Ad(iseffective:Int,isbilling:Int,isbid:Int, iswin:Int,
         adorderid:Int,winprice:Double,adpayment:Double):List[Double] = {

    if (iseffective == 1 && isbilling == 1 && isbid == 1) {
      // 不等于0代表钱已经花出去了
      // 返回:  参与竞价数 竞价成功数  DSP广告消费  DSP广告成本
      if (iseffective == 1 && isbilling == 1 && iswin == 1 && adorderid != 0) {
        List[Double](1, 1, winprice / 1000.0, adpayment / 1000.0)
      } else {
        // 参与竞价失败
        List[Double](1, 0, 0, 0)
      }
    }else{
      List[Double](0,0,0,0)
    }
  }
}
