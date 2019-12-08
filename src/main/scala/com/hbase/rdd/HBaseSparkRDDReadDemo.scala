package com.hbase.rdd

import org.apache.hadoop.hbase.client.{Get, Result}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.spark.sql.SparkSession

/**
  * @title: com.hbase.rdd.HBaseSparkRDDReadDemo
  * @description: TODO
  * @author apktool
  * @date 2019-10-04 11:17
  */
object HBaseSparkRDDReadDemo {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val conf = HBaseConfiguration.create()
    conf.setInt("hbase.zookeeper.property.clientPort", 2181)
    conf.set("hbase.zookeeper.quorum", "localhost")

    val hbaseContext = new HBaseContext(spark.sparkContext, conf)
    try {
      val rdd = spark.sparkContext.parallelize(Array(
        Bytes.toBytes("1"),
        Bytes.toBytes("2")))

      //make sure you import import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
      val getRdd = rdd.hbaseBulkGet[String](hbaseContext, TableName.valueOf("test"), 2,
        record => {
          System.out.println("making Get" + record)
          new Get(record)
        },
        (result: Result) => {

          val it = result.listCells().iterator()
          val b = new StringBuilder

          b.append(Bytes.toString(result.getRow) + ":")

          while (it.hasNext) {
            val cell = it.next()
            val q = Bytes.toString(CellUtil.cloneQualifier(cell))
            b.append("(" + q + "," + Bytes.toString(CellUtil.cloneValue(cell)) + ")")
          }
          b.toString()
        }
      )

      getRdd.collect().foreach(v => println(v))

    } finally {
      spark.sparkContext.stop()
    }

  }
}
