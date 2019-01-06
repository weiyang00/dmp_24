package cn.dmp.tags

import cn.dmp.beans.Trade
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
object AllTags extends Tags{

  override def mkTags(any: Any*): mutable.Map[String, Any] = {

    var map = mutable.Map[String, Any]()

    val trade = any(0).asInstanceOf[Trade]

    val gradesMap = any(1).asInstanceOf[mutable.HashMap[String, String]]

    /**
      * 货物的信息顺序
      * 1.单价
      * 2.规格
      * 3.等级
      * 4.发货地
      * 5.库存量
      *
      *
      */

      if ( trade.price > 0){
        map.put( "price:" +String.valueOf(trade.price), 1)
      }

      if  (StringUtils.isNotEmpty(trade.spec)){
        var spec = trade.spec.trim
          .replace("x", "*")
          .replace("mm", "")
          .replace("cm","0")
          .replace("m","00")
        if (!spec.contains("日"))
        map.put("spec:"+spec, 1)
      }

      if (StringUtils.isNotEmpty(trade.grade)){
        if (gradesMap.contains(trade.grade)){
          map.put("grade:"+  gradesMap.get(trade.grade), 1)
        }

      }

      if (StringUtils.isNotEmpty(trade.productName)) map.put("goodsName:"+trade.productName, 1)

      map
  }


}
