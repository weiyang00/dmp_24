package com.example.report

import com.example.beans.Log
import com.example.utils.RptUtils
import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 媒体报表分析
  * broadcast 广播变量的使用
  */
object AppAnalyseRpt extends App {

  /**
    * inputPath -- 输入路径
    * outputPath -- 输出路径
    * dictFilePath -- 字典文件路径
    */
    val inputPath,outputPath,dictFilePath =
      "/resources/data/"; ""; ""

  // 2 创建sparkconf->sparkContext
  val sparkConf = new SparkConf()
  sparkConf.setAppName(s"${this.getClass.getSimpleName}")
  sparkConf.setMaster("local[*]")
  // RDD 序列化到磁盘 worker与worker之间的数据传输
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sc = new SparkContext(sparkConf)

  // 字典文件
  val dictMap = sc.textFile(dictFilePath).map(line => {
    val fields = line.split("\t", -1)
    (fields(4), fields(1))
  }).collect().toMap


  // 将字典数据广播executor
  val broadcast = sc.broadcast(dictMap)

  // 读取数据
  sc.textFile(inputPath)
    .map(_.split(",", -1))
    .filter(_.length >= 85)
    .map(Log(_)).filter(log => !log.appid.isEmpty || !log.appname.isEmpty)
    .map(log => {
      var newAppName = log.appname
      if (!StringUtils.isNotEmpty(newAppName)) {
        newAppName = broadcast.value.getOrElse(log.appid, "未知")
      }

      val req = RptUtils.caculateReq(log.requestmode, log.processnode)
      val rtb = RptUtils.caculateRtb(log.iseffective, log.isbilling, log.isbid, log.adorderid, log.iswin, log.winprice, log.adpayment)
      val showClick = RptUtils.caculateShowClick(log.requestmode, log.iseffective)

      (newAppName, req ++ rtb ++ showClick)
    }).reduceByKey((list1, list2) => {
    list1.zip(list2).map(t => t._1 + t._2)
  }).map(t => t._1 + "," + t._2.mkString(","))
    .saveAsTextFile(outputPath)

  sc.stop()
}
