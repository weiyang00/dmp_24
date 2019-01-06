package cn.dmp.tags

import cn.dmp.beans.Trade
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.mapred.TableOutputFormat

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

  /**
    * 判断hbase中的表是否存在，如果不存在则创建之
    */
  private val configuration: Configuration = sc.hadoopConfiguration
  configuration.set("hbase.zookeeper.quorum","host-0:2182,host-0:2183,host-0:2184")
    private val connection: Connection = ConnectionFactory.createConnection(configuration)

  val day = "20180106"

  val hBaseTableName = "tb_user_tags"

  private val admin: Admin = connection.getAdmin

  if (!admin.tableExists(TableName.valueOf(hBaseTableName))){
    println(s"$hBaseTableName 不存在" )

    val nameName = new HTableDescriptor(TableName.valueOf(hBaseTableName))


    val descriptor = new HColumnDescriptor(s"day$day")
    nameName.addFamily(descriptor)

    admin.createTable(nameName)

    admin.close()
    connection.close()
  }


  /**
    * 指定key的输出类型
    */
  private val conf = new JobConf(configuration)
  // 指定key的输出类型 -- table 类型
  conf.setOutputFormat(classOf[TableOutputFormat])
  // 指定表名
  conf.set(TableOutputFormat.OUTPUT_TABLE, hBaseTableName)

  /**
    * 加载货物等级规则
    */
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
    }).map{
    case (mobile, userTages) =>{
      var put = new Put(Bytes.toBytes(mobile))

      val str = userTages.map(t => t._1 + ":" + t._2).mkString(",")

      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(s"day$day"), Bytes.toBytes(str))

      (new ImmutableBytesWritable(), put) //  ImmutableBytesWritable() 指的是rewkey
    }

  }.saveAsHadoopDataset(conf)

  sc.stop

}
