package com.criteo.dev.cluster.command

import java.io.{File, PrintWriter}

import com.criteo.dev.cluster._
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.sys.process._

/**
  * Special case of SSH Multi action that runs several hive commands.
  */
@Public
class SshHiveAction(node: Node, ignoreError: Boolean = false) extends HiveAction {

  private final val localFilepath = s"${GeneralUtilities.getHomeDir}/${GeneralUtilities.getTempPrefix}-hivequery"
  private final val remoteFilepath = s"~/${GeneralUtilities.getTempPrefix}-hivequery"  //concurrent hive actions on same node not supported for now

  private val commands = new ListBuffer[String]
  private val logger = LoggerFactory.getLogger(classOf[SshHiveAction])

  def add(action: String): Unit = {
    commands.+=(action)
  }

  def run(): String = {
    val localQueryFile = new File(localFilepath)
    val writer = new PrintWriter(localQueryFile)
    commands.foreach(s => {
      writer.write(s"$s;\n")
      logger.info(s)
    })
    writer.close

    localQueryFile.setExecutable(true)
    localQueryFile.setReadable(true)
    localQueryFile.deleteOnExit()

    ScpAction(None, localFilepath, Some(node), remoteFilepath)
    val ignoreErrorFlag = if (ignoreError) "-hiveconf hive.cli.errors.ignore=true" else ""

    val res = SshAction(node, s"hive $ignoreErrorFlag -f $remoteFilepath", returnResult = true, ignoreError)
    SshAction(node, s"rm $remoteFilepath")
    localQueryFile.delete()
    res
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
