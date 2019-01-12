package cn.dmp.tags

import cn.dmp.beans.{Trade, TradeV2}
import cn.dmp.tags.Tags4User.broadcast
import com.example.utils.MobileUtils
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
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
/**
  * 用户画像
  * 保存到hbase
  */

object Tags4UserV2 extends App {

  val inputPath = "D:\\SpaceJava\\dmp_24\\src\\main\\resources\\data\\yuzhuwood_trade_supply_1-19953.csv"
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

  private val rows: RDD[String] = sc.textFile(inputPath)

  rows.map(row => row.split(",", -1))
    .filter(_.length >= 11)
    .map(TradeV2(_))
    .map(trade => {
      val mobiles: Array[String] = (trade.mobile
        .split(",") ++ trade.corpMobile.split(","))
        .filter(mob => {
          MobileUtils.isFixedPhone(mob) || MobileUtils.isPhone(mob)
        })

      val allMaps = AllV2Tags.mkTags(trade)

      (mobiles.mkString(","), allMaps.toList)
    })
    .filter(_._1.length > 0)
    //将当天的用户信息合并
    .reduceByKey((map1, map2) => {
    (map1 ++ map2).groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2.asInstanceOf[Int])).toList
  })
    .map(al => {
      val map = al._2.toMap
      (al._1 + "=>" + map.mkString(",")).trim
    })
    .saveAsTextFile("D:/test/trades/mobiles")


  sc.stop

}
