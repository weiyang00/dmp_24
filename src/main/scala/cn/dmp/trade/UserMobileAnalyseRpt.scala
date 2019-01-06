package cn.dmp.trade

import java.util.Properties

import cn.dmp.beans.{Log, Trade}
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 用户报表：
  * 判定从网络上抓取的用户是否已经是我司的用户，
  * 并进行画像
  *
  */
object UserMobileAnalyseRpt extends App {


  //    if (args.length != 3) {
  //        println(
  //            """
  //              |cn.dmp.report.AppAnalyseRpt
  //              |参数：
  //              | 输入路径
  //              | 字典文件路径
  //              | 输出路径
  //            """.stripMargin)
  //        sys.exit()
  //    }
  //  val  inputPath = "hdfs://qingdao1.emulian.com:9000/test-weiyang/trades/"
  val inputPath = "D:/anywood_trade_supply_146.csv"
  val dictFilePath = ""
  val outputPath = "hdfs:/test-weiyang/users"

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

  val users = userList.collect().toList

  // 数据库中的mobile字段有的末尾包含一个 \n 字符，需要剔除
  val usermobiles = users.map(user => user.get(16).toString.replaceAll("[\n]", ""))

  // 将字典数据广播executor
  val broadcast = sc.broadcast(usermobiles)

  // 读取数据
  val trades = sc.textFile(inputPath)
    .map(_.split(",", -1))
    .filter(_.length >= 19)
    .map(Trade(_))
    .filter(trade => !trade.mobile.isEmpty && !trade.biz.isEmpty && !broadcast.value.contains(trade.mobile))

  trades.foreach(trade => println(trade.mobile + ","))




  sc.stop
}