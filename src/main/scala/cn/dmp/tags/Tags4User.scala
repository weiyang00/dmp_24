package cn.dmp.tags

import java.util.Properties

import cn.dmp.beans.Trade
import cn.dmp.trade.UserMobileAnalyseRpt.{sc, usermobiles, users}
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by WeiYang on 2019/1/5.
  *
  * @Author: WeiYang
  * @Package cn.dmp.tags
  * @Project: dmp_24
  * @Title:
  * @Description: Please fill description of the file here
  * @Date: 2019/1/5 20:54
  */
object Tags4User extends App {

  val inputPath = "D://test/anywood_trade_supply_146.csv"
  val dictFilePath = ""
  val outputPath = "D:/test/tags-wgoods-user"

  print("inputPath === " + inputPath)
  print("dictFilePath === " + dictFilePath)
  print("outputPath === " + outputPath)

  // 2 创建sparkconf->sparkContext
  val sparkConf = new SparkConf()
  sparkConf.setAppName(s"${this.getClass.getSimpleName}")
  sparkConf.setMaster("local[*]")
  // RDD 序列化到磁盘 worker与worker之间的数据传输
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sc = new SparkContext(sparkConf)

  import org.apache.spark.sql.SQLContext

  val sqlContext = new SQLContext(sc)


  val grads = sc.textFile("src/main/resources/gradeChange").map(grad => {
     val allGrades = grad.split(":")
    val standardGrade: String = allGrades(0)
    val nonstandardGrades: String = allGrades(1)
    (standardGrade, nonstandardGrades)
  })

  val gradeGanso: Array[(String, String)] = grads.collect()

  var gradeMap = mutable.HashMap[String, String]()

  gradeGanso.foreach(gradeGans =>{
    gradeGans._2.split(" ").foreach(gradeMap.put(_,gradeGans._1))
      })

  println(gradeMap)

  // 将字典数据广播executor
  val broadcast = sc.broadcast(gradeMap)

  val trads = sc.textFile(inputPath)
    .map(_.split(",", -1))
    .filter(_.length >= 19)
    .map(Trade(_)).map(trade =>{
    /**
      * 数据进行标签化处理
      */
//    val wgoodsTagsMap = WgoodsTags.mkTags(trade, broadcast.value)
    val allMaps = AllTags.mkTags(trade, broadcast.value)

    (trade.mobile, allMaps.toList)
  })
    .reduceByKey((a, b) =>{
      (a++b).groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2.asInstanceOf[Int])).toList
    }).saveAsTextFile(outputPath)

  sc.stop

}
