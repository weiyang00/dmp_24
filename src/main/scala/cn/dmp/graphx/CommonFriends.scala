package cn.dmp.graphx

import cn.dmp.merge.UserTagsMerge.{dictFilePath, inputPath, outputPath}
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by WeiYang on 2019/1/9.
  *
  * @Author: WeiYang
  * @Package cn.dmp.graphx
  * @Project: dmp_24
  * @Title:
  * @Description: Please fill description of the file here
  * @Date: 2019/1/9 14:07
  */
object CommonFriends extends App {

  val inputPath = "D://test/anywood_trade_supply_146.csv"
  val dictFilePath = ""
  val outputPath = "D:/test/tags-wgoods-user"

  println(s"inputPath    :'$inputPath'")
  println(s"dictFilePath :'$dictFilePath'")
  println(s"outputPath   :'$outputPath'")


  // 2 创建sparkconf->sparkContext
  val sparkConf = new SparkConf()
  sparkConf.setAppName(s"${this.getClass.getSimpleName}")
  sparkConf.setMaster("local[*]")
  // RDD 序列化到磁盘 worker与worker之间的数据传输
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sc = new SparkContext(sparkConf)

  import org.apache.spark.sql.SQLContext

  val sqlContext = new SQLContext(sc)

  // 以一个元祖 作为图中的一个点  Vertex(点)
  private val userValues: RDD[(VertexId, (String, Int))] = sc.parallelize(Seq(
    (1, ("曹操", 23)),
    (2, ("袁绍", 14)),
    (3, ("刘备", 34)),
    (4, ("司马懿", 34)),
    (5, ("关云聪", 134)),
    (6, ("诸葛", 324))
  ))
  userValues

  // 构建边Edge
  private val userRelations = sc.parallelize(Seq(
    Edge(1, 3, 0),
    Edge(1, 4, 0),
    Edge(2, 3, 0),
    Edge(2, 5, 0)
  ))

  private val graph = Graph(userValues, userRelations)

  //  graph.connectedComponents().vertices.foreach(println)

  private val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices

  private val commonFriendsIds: RDD[(VertexId, List[VertexId])] = vertices.map(t => (t._2, List(t._1))).reduceByKey(_ ++ _)

  commonFriendsIds.foreach(println)


  /**
    * userValues --> (userId, (姓名， 年龄))
    * vertices --> (userId， 共同的顶点)
    */
  private val commonFriends = userValues
    .join(vertices)
    .map {
      case (userId, ((name, age), cmId)) =>
        (cmId, List((userId, name, age)))
    }
    .reduceByKey(_ ++ _)
    .map(t => {
      (t._1, t._2.filter(one => one._1 != t._1))
    })

  commonFriends.foreach(println)


  sc.stop

}
