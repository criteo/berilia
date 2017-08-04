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
  private final val localTmpQueryFile = s"${GeneralUtilities.getTempDir}/tmphivequery"

  private val commands = new ListBuffer[String]
  private val logger = LoggerFactory.getLogger(this.getClass)

  def add(action: String): Unit = {
    commands += action
  }

  def run(): String = {
    GeneralUtilities.prepareTempDir
    val localQueryFile = new File(s"${GeneralUtilities.getHomeDir}/$localTmpQueryFile")
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
    val ret = ShellAction(s"hive $ignoreErrorFlag -f $localTmpQueryFile", returnResult = true, ignoreError)
    GeneralUtilities.cleanupTempDir
    ret
  }
}
