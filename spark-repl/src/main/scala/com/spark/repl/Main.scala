package com.spark.repl

import java.io.File
import java.net.URI

import org.apache.spark.SparkConf

import scala.tools.nsc.GenericRunnerSettings

object Main {
  private var hasErrors = false

  def main(args: Array[String]) {
    val jars = getLocalUserJarsForShell(new SparkConf())
      .map { x => if (x.startsWith("file:")) new File(new URI(x)).getPath else x }
      .mkString(File.pathSeparator)

    val interpArguments = List(
      "-Yrepl-class-based",
      "-Yrepl-outdir", s"${Utils.outputDir.getAbsolutePath}",
      "-classpath", jars
    ) ++ args.toList

    val settings = new GenericRunnerSettings(scalaOptionError)
    settings.processArguments(interpArguments, true)
    settings.usejavacp.value = true

    if (!hasErrors) {
      new SparkILoop().process(settings)
      Option(Utils.sparkContext).foreach(_.stop)
    }
  }

  private def scalaOptionError(msg: String): Unit = {
    hasErrors = true
    Console.err.println(msg)
  }

  private def getLocalUserJarsForShell(conf: SparkConf): Seq[String] = {
    val localJars = conf.getOption("spark.repl.local.jars")
    localJars.map(_.split(",")).map(_.filter(_.nonEmpty)).toSeq.flatten
  }
}
