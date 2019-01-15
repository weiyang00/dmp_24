package cn.dmp

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by WeiYang on 2019/1/13.
  *
  * @Author: WeiYang
  * @Package cn.dmp
  * @Project: dmp_24
  * @Title:
  * @Description: Please fill description of the file here
  * @Date: 2019/1/13 10:54
  */
class StartApplication extends App {

  val inputPath = "D:\\SpaceJava\\dmp_24\\src\\main\\resources\\data\\yuzhuwood_trade_supply_1-19953.csv"
  val dictFilePath = ""
  val outputPath = "D:/test/tags-wgoods-user"

  // 2 创建sparkconf->sparkContext
  val sparkConf = new SparkConf()
  sparkConf.setAppName(s"${this.getClass.getSimpleName}")
  sparkConf.setMaster("local[*]")
  // RDD 序列化到磁盘 worker与worker之间的数据传输
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sc = new SparkContext(sparkConf)

  import org.apache.spark.sql.SQLContext

  val sqlContext = new SQLContext(sc)




  sc.stop





}
