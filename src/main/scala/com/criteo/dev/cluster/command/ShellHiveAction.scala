package com.criteo.dev.cluster.command

import java.io.{File, PrintWriter}

import com.criteo.dev.cluster.{GeneralUtilities, Public}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
  * Run several Hive commands on shell
  */
@Public
class ShellHiveAction(ignoreError: Boolean = false) extends HiveAction {
  private val commands = new ListBuffer[String]
  private val logger = LoggerFactory.getLogger(this.getClass)

  private final val filepath = s"${GeneralUtilities.getHomeDir}/${GeneralUtilities.getTempPrefix}-hivequery"

  def add(action: String): Unit = {
    commands += action
  }

  def run(): String = {
    val localQueryFile = new File(filepath)
    val writer = new PrintWriter(localQueryFile)
    commands.foreach(s => {
      writer.write(s"$s;\n")
      logger.info(s)
    })
    writer.close

    localQueryFile.setExecutable(true)
    localQueryFile.setReadable(true)
    localQueryFile.deleteOnExit()

    val ignoreErrorFlag = if (ignoreError) "-hiveconf hive.cli.errors.ignore=true" else ""
    ShellAction(s"hive $ignoreErrorFlag -f $filepath", returnResult = true, ignoreError)
  }
}
