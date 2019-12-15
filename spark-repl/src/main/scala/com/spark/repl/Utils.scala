package com.spark.repl

import java.io.File
import java.util.Locale

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @package com.spark.repl
  * @class Utils
  * @description TODO
  * @author apktool
  * @date 2019-12-15 15:12
  */
object Utils extends Logging {
  val outputDir = new File("/tmp", "test").getCanonicalFile

  var sparkContext: SparkContext = _
  var sparkSession: SparkSession = _

  def createSparkSession(): SparkSession = {
    val conf = new SparkConf()
    try {
      val execUri = System.getenv("SPARK_EXECUTOR_URI")
      conf.setIfMissing("spark.repl.class.outputDir", outputDir.getAbsolutePath())
      conf.setIfMissing("spark.testing.memory", "2147480000")
      conf.setIfMissing("spark.master","local[4]")
      conf.setIfMissing("spark.app.name", "Spark Shell Demo")

      if (execUri != null) {
        conf.set("spark.executor.uri", execUri)
      }
      if (System.getenv("SPARK_HOME") != null) {
        conf.setSparkHome(System.getenv("SPARK_HOME"))
      }

      val builder = SparkSession.builder.config(conf)

      sparkSession = builder.getOrCreate()

      if (conf.get(CATALOG_IMPLEMENTATION.key, "hive").toLowerCase(Locale.ROOT) == "hive") {
        if (hiveClassesArePresent) {
          sparkSession = builder.enableHiveSupport().getOrCreate()
        } else {
          builder.config(CATALOG_IMPLEMENTATION.key, "in-memory")
          sparkSession = builder.getOrCreate()
        }
      }
      val msg = if (hiveClassesArePresent) " with Hive support" else ""
      logInfo("Created Spark session" + msg)

      sparkContext = sparkSession.sparkContext
      sparkSession
    } catch {
      case e: Exception =>
        logError("Failed to initialize Spark session.", e)
        sys.exit(1)
    }
  }

  private def hiveClassesArePresent: Boolean = {
    try {
      Class.forName(
        "org.apache.spark.sql.hive.HiveSessionStateBuilder",
        true,
        Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
      )
      Class.forName(
        "org.apache.hadoop.hive.conf.HiveConf"
      )
      true
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError => false
    }
  }
}
