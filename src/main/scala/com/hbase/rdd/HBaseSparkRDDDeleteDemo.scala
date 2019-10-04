package com.hbase.rdd

import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.SparkSession

/**
  * @title: com.hbase.rdd.HBaseSparkRDDDeleteDemo
  * @description: TODO
  * @author apktool
  * @date 2019-10-04 19:48
  */
object HBaseSparkRDDDeleteDemo {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("HBaseSparkRDDDeleteDemo")
      .getOrCreate()

    val hbaseConf = HBaseConfiguration.create()

    val hbaseContext = new HBaseContext(spark.sparkContext, hbaseConf)

    try {
      val rdd = spark.sparkContext.parallelize(Array(
        Bytes.toBytes("1"),
        Bytes.toBytes("2")
      ))

      //make sure you import import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
      rdd.hbaseBulkDelete(hbaseContext, TableName.valueOf("employee"),
        putRecord => new Delete(putRecord),
        4)

      //Alternatively you can also write as below
      //      hbaseContext.bulkDelete[Array[Byte]](rdd,
      //        TableName.valueOf("employee"),
      //        putRecord => new Delete(putRecord),
      //        4)

    } finally {
      spark.sparkContext.stop()
    }
  }
}
