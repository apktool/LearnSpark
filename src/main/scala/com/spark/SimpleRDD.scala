package com.spark

import org.apache.spark.{SparkConf, SparkContext}

object SimpleRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SimpleRDD").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val number = 0 to 10

    // put some data in an RDD
    val numberRDD = sc.parallelize(number, 4)

    println("Print each element of the original RDD")
    numberRDD.foreach(t => print(t + " "))

    // trivially operate on the numbers
    val stillAnRDD = numberRDD.map(t => t.toDouble / 2)
    stillAnRDD.foreach(t => print(t + " "))

    // get the data back out
    val nowAnArray = stillAnRDD.collect()
    println("Now print each element of the transformed array")
    nowAnArray.foreach(t => print(t + " "))

    val partitions = stillAnRDD.glom()
    println("We _should_ have 4 partitions")
    println(partitions.count())

    partitions.foreach(t => {
      println("Partition contents:" + t.foldLeft("")((s, e) => s + " " + e))
    })
  }
}
