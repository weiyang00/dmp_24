package cn.dmp.tags

import cn.dmp.beans.{Trade, TradeV2}
import org.apache.commons.lang.StringUtils

import scala.collection.mutable


/**
  * Created by WeiYang on 2019/1/5.
  *
  * @Author: WeiYang
  * @Package cn.dmp.tags
  * @Project: dmp_24
  * @Title:
  * @Description: Please fill description of the file here
  * @Date: 2019/1/5 21:02
  */
object AllV2Tags extends Tags {

  override def mkTags(any: Any*): mutable.Map[String, Any] = {

    var map = mutable.Map[String, Any]()

    val trade = any(0).asInstanceOf[TradeV2]

    /**
      * 货物的信息顺序
      * 1.买方卖方
      * 2.品名
      * 3.分类
      * 4.交易商名称
      *
      */

    if (StringUtils.isNotEmpty(trade.buyOrSeller)) {
      val tradeRole =
        trade.buyOrSeller match {
          case "买" => "BUYER"
          case "卖" => "SELLER"
          case _ => ""
        }
      StringUtils.isNotEmpty(tradeRole)
      map.put("BS:" + tradeRole, 1)
    }

    if (StringUtils.isNotEmpty(trade.productName)) {
      map.put("PN:" + trade.productName, 1)
    }

    if (StringUtils.isNotEmpty(trade.category)) {
      map.put("CG:" + trade.category, 1)
    }

    if (StringUtils.isNotEmpty(trade.dealerName)) map.put("DN:" + trade.dealerName, 1)

    map
  }


}
