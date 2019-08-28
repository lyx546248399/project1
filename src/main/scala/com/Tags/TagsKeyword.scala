package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

/**
  * 关键字
  * 将停用词库广播进来
  * 5)	关键字（标签格式：Kxxx->1）xxx 为关键字，关键字个数不能少于 3 个字符，且不能
  * 超过 8 个字符；关键字中如包含‘‘|’’，则分割成数组，转化成多个关键字标签
  */
object TagsKeyword extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    // 广播过来的停用词库
    val stopword = args(1).asInstanceOf[Map[String, Int]]

    // 获取关键字,打标签
    val kw = row.getAs[String]("keywords").split("\\|")

    // 按照过滤条件进行过滤数据
    kw.filter(word => {
      // !stopword.contains(word) 过滤出来的单词还必须不能是停用词库里的词
      word.length >=3 && word.length <=8 && !stopword.contains(word)
    })
        .foreach(word => list:+=("K"+word,1))

    list
  }
}
