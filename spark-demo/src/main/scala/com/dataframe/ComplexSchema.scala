package com.dataframe

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @package com.dataframe
  * @class ComplexSchema
  * @description TODO
  * @author apktool
  * @date 2019-12-29 22:30
  */
/*
export MASTER=yarn
/home/li/Software/spark-2.4.4-bin-without-hadoop-scala-2.12/bin/spark-submit --class com.dataframe.ComplexSchema \
    --deploy-mode client \
    --driver-memory 1g \
    --executor-memory 2g \
    --executor-cores 1 \
    /home/li/WorkSpace/LearnSpark/spark-demo/target/spark-demo-1.0-SNAPSHOT.jar \
    10
 */

object ComplexSchema {
  def main(args: Array[String]): Unit = {
    val env = sys.env
    val master = env.get("MASTER")
      .orElse(env.get("spark.master"))
      .getOrElse("yarn")

    val spark = SparkSession.builder()
      .master(master)
      .appName(getClass.getCanonicalName)
      .getOrCreate()

    import spark.implicits._

    //
    // Example 1: nested StructType for nested rows
    //

    val rows1 = Seq(
      Row(1, Row("a", "b"), 8.00, Row(1, 2)),
      Row(2, Row("c", "d"), 9.00, Row(3, 4))

    )
    val rows1Rdd = spark.sparkContext.parallelize(rows1, 4)

    val schema1 = StructType(
      Seq(
        StructField("id", IntegerType, true),
        StructField("s1", StructType(
          Seq(
            StructField("x", StringType, true),
            StructField("y", StringType, true)
          )
        ), true),
        StructField("d", DoubleType, true),
        StructField("s2", StructType(
          Seq(
            StructField("u", IntegerType, true),
            StructField("v", IntegerType, true)
          )
        ), true)
      )
    )

    println("Position of subfield 'd' is " + schema1.fieldIndex("d"))

    val df1 = spark.createDataFrame(rows1Rdd, schema1)

    println("Schema with nested struct")
    df1.printSchema()

    println("DataFrame with nested Row")
    df1.show()

    println("Select the column with nested Row at the top level")
    df1.select("s1").show()

    println("Select deep into the column with nested Row")
    df1.select("s1.x").show()

    println("The column function getField() seems to be the 'right' way")
    df1.select($"s1".getField("x")).show()

    //
    // Example 2: ArrayType
    //

    val rows2 = Seq(
      Row(1, Row("a", "b"), 8.00, Array(1, 2)),
      Row(2, Row("c", "d"), 9.00, Array(3, 4, 5))

    )
    val rows2Rdd = spark.sparkContext.parallelize(rows2, 4)

    //
    // This time, instead of just using the StructType constructor, see
    // that you can use two different overloads of the add() method to add
    // the fields 'd' and 'a'
    //
    val schema2 = StructType(
      Seq(
        StructField("id", IntegerType, true),
        StructField("s1", StructType(
          Seq(
            StructField("x", StringType, true),
            StructField("y", StringType, true)
          )
        ), true)
      )
    )
      .add(StructField("d", DoubleType, true))
      .add("a", ArrayType(IntegerType))

    val df2 = spark.createDataFrame(rows2Rdd, schema2)

    println("Schema with array")
    df2.printSchema()

    println("DataFrame with array")
    df2.show()

    // interestingly, indexing using the [] syntax seems not to be supported
    // (surprising only because that syntax _does_ work in Spark SQL)
    //df2.select("id", "a[2]").show()

    println("Use column function getItem() to index into array when selecting")
    df2.select($"id", $"a".getItem(2)).show()

    //
    // Example 3: MapType
    //

    val rows3 = Seq(
      Row(1, 8.00, Map("u" -> 1, "v" -> 2)),
      Row(2, 9.00, Map("x" -> 3, "y" -> 4, "z" -> 5))
    )
    val rows3Rdd = spark.sparkContext.parallelize(rows3, 4)

    val schema3 = StructType(
      Seq(
        StructField("id", IntegerType, true),
        StructField("d", DoubleType, true),
        StructField("m", MapType(StringType, IntegerType))
      )
    )

    val df3 = spark.createDataFrame(rows3Rdd, schema3)

    println("Schema with map")
    df3.printSchema()

    println("DataFrame with map")
    df3.show()

    // MapType is actually a more flexible version of StructType, since you
    // can select down into fields within a column, and the rows where
    // an element is missing just return a null
    println("Select deep into the column with a Map")
    df3.select($"id", $"m.u").show()

    println("The column function getItem() seems to be the 'right' way")
    df3.select($"id", $"m".getItem("u")).show()

    spark.close()
  }
}
