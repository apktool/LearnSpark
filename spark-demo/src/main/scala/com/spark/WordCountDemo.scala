package com.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCountDemo {
  def main(args: Array[String]): Unit = {
    val input = "/tmp/input"
    val output = "/tmp/output"

    val conf = new SparkConf()
      .setAppName("word count")
      .setMaster("local[4]")  // 打成jar包使用时，需要注释此行。该设置与shell.sh的配置冲突

    val sc = new SparkContext(conf)

    val data = sc.textFile(input)

    val cc = data.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    cc.saveAsTextFile(output)

    sc.stop()
  }
}
