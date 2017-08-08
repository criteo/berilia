package com.criteo.dev.cluster.command

import java.io.{File, PrintWriter}

import com.criteo.dev.cluster._
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.sys.process.ProcessLogger

/**
  * Collects a bunch of commands into a shell script and executes it remotely.
  * <p>
  * Could use a 'here document' but there were issues with that.
  */
@Public
case class SshMultiAction(node: Node) extends MultiAction {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val commands = new ListBuffer[String]

  //to allow concurrency
  val localTmpShell = s"${GeneralUtilities.getTempDir}/tmp.sh"
  val remoteTmpShell = s"${GeneralUtilities.getTempDir}-tmp.sh"

  def add(command : String): Unit = {
    commands.+=(command)
  }

  def run(returnResult: Boolean = false, ignoreError: Boolean = false) : String = {
    //cleanup tmp files, ignore errors in these commands.
    GeneralUtilities.prepareTempDir
    val localTmpShellFile = new File(s"${GeneralUtilities.getHomeDir}/$localTmpShell")
    SshAction(node, " rm " + remoteTmpShell, returnResult = false, true)

    //Write a temp shell script
    val writer = new PrintWriter(localTmpShellFile)
    commands.foreach(s => writer.write(s"$s\n"))
    writer.close

    localTmpShellFile.setExecutable(true)
    localTmpShellFile.setReadable(true)
    localTmpShellFile.deleteOnExit()

    commands.foreach(s => logger.info(s))

    ScpAction(None, localTmpShell, Some(node), remoteTmpShell)
    val result = SshAction(node, " source " + remoteTmpShell, returnResult, ignoreError)
    SshAction(node, " rm " + remoteTmpShell, returnResult = false, true)
    GeneralUtilities.cleanupTempDir
    result
  }
}

object SshMultiAction {
  def apply(node: Node,
            commands: List[String],
            returnResult: Boolean = false,
            ignoreError: Boolean = false) : String = {
    val action = new SshMultiAction(node)
    commands.foreach(action.add)
    action.run(returnResult = returnResult, ignoreError = ignoreError)
  }
}
