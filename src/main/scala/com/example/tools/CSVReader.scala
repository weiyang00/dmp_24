package com.example.tools


import java.io.{StringReader, StringWriter}

import au.com.bytecode.opencsv.{CSVReader, CSVWriter}
import com.example.StartApplication
import org.apache.spark.SparkContext

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
object CSVReader extends StartApplication {

  override def execute(sparkContext: SparkContext): Unit = {
    val input = sparkContext.textFile("D://cfgc_news_industry_525.csv")
    input.collect().foreach(println)
    val result = input.map { line =>
      val reader = new CSVReader(new StringReader(line))
      val writer = new CSVWriter(new StringWriter())
//            writer.writeAll()

      reader.readNext()
    }
    println(result.getClass)
    result.collect().foreach(x => {
      x.foreach(println)
      println("======")
    })


  }


}
