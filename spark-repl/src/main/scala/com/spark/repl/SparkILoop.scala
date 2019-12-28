package com.spark.repl

import java.io.BufferedReader

import scala.tools.nsc.interpreter._
import scala.util.Properties.{javaVersion, javaVmName, versionString}

class SparkILoop(in: Option[BufferedReader], out: JPrintWriter) extends ILoop(in, out) {

  def this() = this(None, new JPrintWriter(Console.out, true))

  def this(in: BufferedReader, out: JPrintWriter) = this(Some(in), out)

  override def commands: List[LoopCommand] = standardCommands

  override def resetCommand(line: String): Unit = {
    super.resetCommand(line)
    initializeSpark()
    echo("Note that after :reset, state of SparkSession and SparkContext is unchanged.")
  }

  override def replay(): Unit = {
    initializeSpark()
    super.replay()
  }

  def initializeSpark(): Unit = {
    if (!intp.reporter.hasErrors) {
      savingReplayStack {
        initializationCommands.foreach(intp quietRun _)
      }
    } else {
      throw new RuntimeException(s"Scala $versionString interpreter encountered errors during initialization")
    }
  }

  override def printWelcome() {
    import org.apache.spark.SPARK_VERSION

    import scala.Predef.{println => _}

    echo(
      """Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version %s
      /_/
         """.format(SPARK_VERSION))
    val welcomeMsg = "Using Scala %s (%s, Java %s)".format(
      versionString, javaVmName, javaVersion)
    echo(welcomeMsg)
    echo("Type in expressions to have them evaluated.")
    echo("Type :help for more information.")
  }

  val initializationCommands: Seq[String] = Seq(
    "import org.apache.spark.SparkContext._",
    "import org.apache.spark.sql.functions._",
    "import org.apache.spark.sql",
    "import com.spark.repl.Utils",
    """
    @transient val spark = if (Utils.sparkSession != null) {
        Utils.sparkSession
      } else {
        Utils.createSparkSession()
      }
    @transient val sc = {
      val _sc = spark.sparkContext
      if (_sc.getConf.getBoolean("spark.ui.reverseProxy", false)) {
        val proxyUrl = _sc.getConf.get("spark.ui.reverseProxyUrl", null)
        if (proxyUrl != null) {
          println(
            s"Spark Context Web UI is available at ${proxyUrl}/proxy/${_sc.applicationId}")
        } else {
          println(s"Spark Context Web UI is available at Spark Master Public URL")
        }
      } else {
        _sc.uiWebUrl.foreach {
          webUrl => println(s"Spark context Web UI available at ${webUrl}")
        }
      }
      println("Spark context available as 'sc' " +
        s"(master = ${_sc.master}, app id = ${_sc.applicationId}).")
      println("Spark session available as 'spark'.")
      _sc
    }
    """
  )
}
