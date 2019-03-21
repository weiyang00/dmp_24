package com.example.trade

import java.util.Properties

import com.example.beans.{Log, Trade}
import com.example.utils
import com.example.utils.{MobileUtils, StringNum}
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import scala.collection.mutable


/**
  * 用户报表：
  * 判定从网络上抓取的用户是否已经是我司的用户，
  * 并进行画像
  *
  */
object UserMobileAnalyseRpt extends App {


  val inputPath = "C:\\Users\\weiyang\\Desktop\\手机号\\mobile"
  val dictFilePath = ""
  val outputPath = "hdfs:/test-weiyang/users"

  println("inputPath === " + inputPath)
  println("dictFilePath === " + dictFilePath)
  println("outputPath === " + outputPath)

  // 2 创建sparkconf->sparkContext
  val sparkConf = new SparkConf()
  sparkConf.setAppName(s"${this.getClass.getSimpleName}")
  sparkConf.setMaster("local[*]")
  // RDD 序列化到磁盘 worker与worker之间的数据传输
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sc = new SparkContext(sparkConf)

  val sqlContext = new SQLContext(sc)


  // 加载配置文件  application.conf -> application.json --> application.properties
  val load = ConfigFactory.load()
  val props = new Properties()
  props.setProperty("user", load.getString("jdbc.user"))
  props.setProperty("password", load.getString("jdbc.password"))
  // 字典文件、从mysql中读取已有的用户，最终保存为list
  //    val dictMap = sc.textFile(dictFilePath).map(line => {
  //        val fields = line.split("\t", -1)
  //        (fields(4), fields(1))
  //    }).collect().toMap
  val url = s"${load.getString("jdbc.url")}&user=${load.getString("jdbc.user")}&password=${load.getString("jdbc.password")}"
  val userList = sqlContext.read.format("jdbc").option("url", url).option("dbtable", load.getString("jdbc.tableName")).load()

  val users = userList.map(user => user.get(16).toString.replaceAll("[\n]", "")).collect().toList

  // 数据库中的mobile字段有的末尾包含一个 \n 字符，需要剔除
  //  val usermobiles = users.map(user => user.get(16).toString.replaceAll("[\n]", ""))

  private val stringToString1 = new mutable.HashMap[String, Int]

  users.foreach(stringToString1.put(_, 1))

  // 将字典数据广播executor
  val broadcast = sc.broadcast(stringToString1)

  // 读取数据
  sc.textFile(inputPath)
    .flatMap(_.split(",", -1))
    .map(StringNum.getNum)
    .filter(str => MobileUtils.isFixedPhone(str) || MobileUtils.isPhone(str))
    .sortBy(str => str)
    .map((_, 1))
    .reduceByKey(_ + _)
    .map(mob => {
      val stringToString = broadcast.value.asInstanceOf[mutable.HashMap[String, String]]
      var i = 0
      if (stringToString.contains(mob._1)) {
        i = 1
      }
      mob._1 + "," + mob._2 + "," + i
    })
    .repartition(1).saveAsTextFile("D:\\手机号")

  //  trades.foreach(trade => println(trade.mobile + ","))


  sc.stop
}