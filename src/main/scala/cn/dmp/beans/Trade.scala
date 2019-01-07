package cn.dmp.beans

import cn.dmp.utils.NBF

/**
  * Created by WeiYang on 2019/1/3.
  *
  * @Author: WeiYang
  * @Package cn.dmp.beans
  * @Project: dmp_24
  * @Title:
  * @Description: Please fill description of the file here
  * @Date: 2019/1/3 19:33
  */
class Trade(
             val title: String,
             val no: Long,
             val biz: String,
             val price: Double,
             val productName: String,
             val unit: String,
             val brand: String,
             val spec: String,
             val deliveryWarehouse: String,
             val grade: String,
             val salesVolume: Int,
             val mobile: String,
             val freight: String,
             val repertory: Long,
             val img1: String,
             val img2: String,
             val img3: String,
             val website: String,
             val details: String
           ) extends Product with Serializable {

  // 角标和成员属性的映射关系
  override def productElement(n: Int): Any = n match {
    case 0 => title
    case 1 => no
    case 2 => biz
    case 3 => price
    case 4 => productName
    case 5 => unit
    case 6 => brand
    case 7 => spec
    case 8 => deliveryWarehouse
    case 9 => grade
    case 10 => salesVolume
    case 11 => mobile
    case 12 => freight
    case 13 => repertory
    case 14 => img1
    case 15 => img2
    case 16 => img3
    case 17 => website
    case 18 => details
  }

  // 对象一个又多少个成员属性
  override def productArity: Int = 18

  // 比较两个对象是否是同一个对象
  override def canEqual(that: Any): Boolean = that.isInstanceOf[Trade]
}

object Trade {
  def apply(arr: Array[String]): Trade = new Trade(
    arr(0),
    NBF.toLong(arr(1)),
    arr(2),
    NBF.toDouble(arr(3)),
    arr(4),
    arr(5),
    arr(6),
    arr(7),
    arr(8),
    arr(9),
    NBF.toInt(arr(10)),
    arr(11),
    arr(12),
    NBF.toLong(arr(13)),
    arr(14),
    arr(15),
    arr(16),
    arr(17),
    arr(18)
  )
}
