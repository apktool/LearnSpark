package com.spark

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * @title: com.spark.PropertiesConfigDemo
  * @description: Read external properties
  * @author apktool
  * @date 2019-10-04 20:05
  */
object PropertiesConfigDemo {
  final val logger = LoggerFactory.getLogger(PropertiesConfigDemo.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .getOrCreate()

    val conf = spark.sparkContext.getConf

    logger.info("spark.app.master -> " + conf.get("spark.app.master"))
    logger.info("spark.app.name-> " + conf.get("spark.app.name"))

    println(conf.get("spark.app.master"))
    println(conf.get("spark.app.name"))

    spark.close()
  }
}
