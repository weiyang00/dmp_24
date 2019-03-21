package com.example.merge

import com.example.beans.TradeV2
import com.example.utils.MobileUtils
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import sun.security.provider.certpath.Vertex

/**
  * Created by WeiYang on 2019/1/5.
  *
  * @Author: WeiYang
  * @Package com.example.tags
  * @Project: dmp_24
  * @Title:
  * @Description: Please fill description of the file here
  * @Date: 2019/1/5 20:54
  */
/**
  * 相同用户标签合并merge
  */

object UserTagsMerge extends App {

  val inputPath = "D:\\code\\java\\dmp_24\\src\\main\\resources\\data\\mobiles"
  val dictFilePath = ""
  val outputPath = "D:/test/tags-wgoods-user"

  println(s"inputPath    : '$inputPath'")
  println(s"dictFilePath : '$dictFilePath'")
  println(s"outputPath   : '$outputPath'")

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
    * 1.根据数据创建出点的集合
    * 2.创建出边的集合
    * 3.创建一个图
    * 4.整合数据
    *
    */
  private val rows: RDD[String] = sc.textFile(inputPath)

  // 1.根据数据创建出点的集合
  private val uv = rows.flatMap(row => {
    val strings = row.split("=>")
    val mobiles = strings(0).split(",")
    val tags = strings(1).split(",").map(kvStr => {
      val kv = kvStr.split("->")
      (kv(0), kv(1).trim.toInt)
    }).toList

    mobiles.map(mobile => {
      // 转换成顶点时，只同一行的第一个人带标签，同一行的其他人无标签
      if (mobile.equals(mobiles(0))) {
        (mobile.hashCode.toLong, (mobile, tags))
      } else {
        (mobile.hashCode.toLong, (mobile, List.empty[(String, Int)]))
      }
    })
  })

  // 2.创建出边的集合
  private val ue: RDD[Edge[Int]] = rows.flatMap(row => {
    val strings = row.split("=>")
    val mobiles = strings(0).split(",")

    mobiles.map(moblie => Edge(mobiles(0).hashCode.toLong, moblie.hashCode.toLong, 0))
  })

  // 3.创建一个图
  val graph = Graph(uv, ue)
  // return : (id, 共同的最小id的顶点)
  val cc = graph.connectedComponents().vertices

  // 4.聚合数据
  cc.join(uv).map {
    // （顶点的id， （连通图中的最小id的顶点，（手机号码， 标签数组））） => ( 连通图中的最小id的顶点, (手机号， 标签数组))
    case (id, (minId, (mobile, tags))) => (minId, (List(mobile), tags))
  }.reduceByKey {
    case (t1, t2) => {
      val k = (t1._1 ++ t2._1).distinct
      val v = (t1._2 ++ t2._2).groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2)).toList
      (k, v)
    }
  }.map(t => (t._2._1, t._2._2)).foreach(println)


  sc.stop

}
