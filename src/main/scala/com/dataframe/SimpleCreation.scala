package com.dataframe

import org.apache.spark.sql.SparkSession

object SimpleCreation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrame-SimpleCreatioin")
      .master("local[4]")
      .getOrCreate()

    // create an RDD of tuples with some data
    val custs = Seq(
      (1, "Widget Co", 120000.00, 0.00, "AZ"),
      (2, "Acme Widgets", 410500.00, 500.00, "CA"),
      (3, "Widgetry", 410500.00, 200.00, "CA"),
      (4, "Widgets R Us", 410500.00, 0.0, "CA"),
      (5, "Ye Olde Widgete", 500.00, 0.0, "MA")
    )

    val customerRows = spark.sparkContext.parallelize(custs, 4)

    import spark.implicits._

    // convert RDD of tuples to DataFrame by supplying column names
    val customerDF = customerRows.toDF("id", "name", "sales", "discount", "state")

    customerDF.printSchema()
    customerDF.show()
  }
}
