package com.criteo.dev.cluster

import java.io.{File, PrintWriter}

import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.sys.process._

/**
  * Special case of SSH Multi action that runs several hive commands.
  */
@Public
class SshHiveAction(node: Node, ignoreError: Boolean = false) {

  private final val localTmpQueryFile = s"${GeneralUtilities.getTempDir}/tmphivequery"
  private final val remoteTmpQueryFile = s"./tmphivequery"  //concurrent hive actions on same node not supported for now

  private val commands = new ListBuffer[String]
  private val logger = LoggerFactory.getLogger(classOf[SshHiveAction])

  private val processLogger = ProcessLogger(
    (e: String) => logger.info("err " + e))

  def add(action: String): Unit = {
    commands.+=(action)
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

    ScpAction(None, localTmpQueryFile, Some(node), remoteTmpQueryFile)
    val ignoreErrorFlag = if (ignoreError) "-hiveconf hive.cli.errors.ignore=true" else ""

    val ret = SshAction(node, s"hive $ignoreErrorFlag -f $remoteTmpQueryFile", returnResult = true, ignoreError)
    GeneralUtilities.cleanupTempDir
    ret
  }

  override def toString = {
    commands.mkString("\n")
  }
}

object SshHiveAction {
  def apply(node: Node, statements: List[String], ignoreError: Boolean = false) = {
    val action = new SshHiveAction(node, ignoreError)
    statements.foreach(action.add)
    action.run
  }
}
