package com.example.beans

import com.example.utils.NBF

/**
  * Created by WeiYang on 2019/1/3.
  *
  * @Author: WeiYang
  * @Package com.example.beans
  * @Project: dmp_24
  * @Title:
  * @Description: Please fill description of the file here
  * @Date: 2019/1/3 19:33
  */
class TradeV2(
               val no: String,
               val buyOrSeller: String,
               val date: String,
               val title: String,
               val productName: String,
               val category: String,
               val info: String,
               val dealerCode: String,
               val dealerName: String,
               val connector: String,
               val corpMobile: String,
               val mobile: String,
               val fax: String,
               val mail: String,
               val site: String
             ) extends Product with Serializable {

  // 角标和成员属性的映射关系
  override def productElement(n: Int): Any = n match {
    case 0 => no
    case 1 => buyOrSeller
    case 2 => date
    case 3 => title
    case 4 => productName
    case 5 => category
    case 6 => info
    case 7 => dealerCode
    case 8 => dealerName
    case 9 => connector
    case 10 => corpMobile
    case 11 => mobile
    case 12 => fax
    case 13 => mail
    case 14 => site
  }

  // 对象一个又多少个成员属性
  override def productArity: Int = 15

  // 比较两个对象是否是同一个对象
  override def canEqual(that: Any): Boolean = that.isInstanceOf[Trade]
}


object TradeV2 {
  def apply(arr: Array[String]): TradeV2 = new TradeV2(
    arr(0),
    arr(1),
    arr(2),
    arr(3),
    arr(4),
    arr(5),
    arr(6),
    arr(7),
    arr(8),
    arr(9),
    arr(10),
    arr(11),
    arr(12),
    arr(13),
    arr(13)
  )
}



