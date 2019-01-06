package cn.dmp.tags

import scala.collection.mutable

/**
  * Created by WeiYang on 2019/1/5.
  *
  * @Author: WeiYang
  * @Package cn.dmp.tags
  * @Project: dmp_24
  * @Title:
  * @Description: Please fill description of the file here
  * @Date: 2019/1/5 21:00
  */
trait Tags {

  def mkTags(any: Any*) : mutable.Map[String, Any]


}
