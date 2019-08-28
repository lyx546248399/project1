package exercise

import scala.collection.mutable

object test {
  def main(args: Array[String]): Unit = {
//    val scores = mutable.Map("lisi" -> 80,"wangwu" -> 75, "zhangsan" -> 96)
//    val sc = scores("lisi") = 75
//    println(scores("lisi"))
//    println(if(scores.contains("lisi")) scores("lisi") else 0)

//    var map4 = mutable.Map( ("A", 1), ("B", "北京"), ("C", 3) )
//    println(map4.get("A"))
//    println(map4.get("A").get)

//    Bloom Filter是一种空间效率很高的随机数据结构，它利用位数组很简洁地表示一个集合，
    //    并能判断一个元素是否属于这个集合。Bloom Filter的这种高效是有一定代价的：
    //    在判断一个元素是否属于某个集合时，有可能会把不属于这个集合的元素误认为
    //    属于这个集合（false positive）。因此，Bloom Filter不适合那些“零错误”的应用场合。
    //    而在能容忍低错误率的应用场合下，Bloom Filter通过极少的错误换取了存储空间的极大节省
//      高效地插入和查询
//    布隆过滤器可以支持 add 和 isExist 操作,不支持delete操作
//    利用布隆过滤器减少磁盘 IO 或者网络请求，因为一旦一个值必定不存在的话，
    //    我们可以不用进行后续昂贵的查询请求。


    val lst = List(1,2,3,4,5)
    lst.foreach { x => print(x+",")}
    var temp = lst.map { x => x+1 }   //遍历，与foreach的区别是返回值为List【B】
    println("map遍历："+temp.mkString(","))
    println("***************")
    var temp1 = lst.reduceLeft((sum,i)=>sum +i) //遍历，返回值类型是一个与集合相同的Int
    println("reduce遍历返回Int："+temp1)
//    var temp2 = lst.foldLeft(0)((x,y)=>y::x)
//    println("foldLeft遍历返回自定义类型："+temp2)

  }
}
