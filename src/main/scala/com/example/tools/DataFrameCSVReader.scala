package com.example.tools

import java.io.{StringReader, StringWriter}

import au.com.bytecode.opencsv.{CSVReader, CSVWriter}
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
object DataFrameCSVReader extends StartApplication {

  override def execute(sparkContext: SparkContext): Unit = {
    val sqlContext = new SQLContext(sparkContext)
//    import sqlContext.implicits._
//    val input = sparkContext.textFile("D://cfgc_news_industry_525.csv").toDF()

    //读csv文件
    val data: DataFrame = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "false") //在csv第一行有属性"true"，没有就是"false"
      .option("inferSchema", true.toString) //这是自动推断属性列的数据类型
      .load("src/main/resources/data/anywood_trade_supply_146.csv")
    //    data.show()

    data.repartition(1).write.format("com.databricks.spark.csv")
      .option("header", "false")//在csv第一行有属性"true"，没有就是"false"
      .option("delimiter",",")//默认以","分割
      .save("src/main/resources/data/anywood_trade_supply_146-output.csv")
  }


}
