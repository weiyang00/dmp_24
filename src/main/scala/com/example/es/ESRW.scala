package com.example.es

import com.example.StartApplication
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by WeiYang on 3/1/2019.
  *
  * @Author: WeiYang
  * @Package com.example.tools
  * @Project: dmp_24
  * @Title:
  * @Description: Please fill description of the file here
  * @Date: 3/1/2019 9:11 AM
  */
object ESRW extends StartApplication {

  override def execute(sparkContext: SparkContext): Unit = {
    val sqlContext = new SQLContext(sparkContext)
//    import sqlContext.implicits._
//    val input = sparkContext.textFile("D://cfgc_news_industry_525.csv").toDF()
    import org.elasticsearch.spark._
    import org.elasticsearch.spark.sql._

    // 读取es
    val esValue = sparkContext.esRDD("radio/artists", "?q=me*")

    // 读取es Spark Sql
    // DataFrame schema automatically inferred
    val df = sqlContext.read.format("es").load("buckethead/albums")

    // operations get pushed down and translated at runtime to Elasticsearch QueryDSL
    val playlist = df.filter(df("category").equalTo("pikes").and(df("year").geq(2016)))

    // 写入 es
    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")
    sparkContext.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")

    // 写入 es Spark Sql
    val df2 = sqlContext.read.json("examples/people.json")
    df2.saveToEs("spark/people")

  }

}
