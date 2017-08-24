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
  val localFilepath = s"${GeneralUtilities.getHomeDir}/${GeneralUtilities.getTempPrefix}.sh"
  val remoteFilePath = s"${GeneralUtilities.getHomeDir}/${GeneralUtilities.getTempPrefix}.sh"

  def add(command : String): Unit = {
    commands.+=(command)
  }

  def run(returnResult: Boolean = false, ignoreError: Boolean = false) : String = {
    val localTmpShellFile = new File(localFilepath)
    SshAction(node, " rm " + remoteFilePath, returnResult = false, true)

    //Write a temp shell script
    val writer = new PrintWriter(localTmpShellFile)
    commands.foreach(s => writer.write(s"$s\n"))
    writer.close

    localTmpShellFile.setExecutable(true)
    localTmpShellFile.setReadable(true)
    localTmpShellFile.deleteOnExit()

    commands.foreach(s => logger.info(s))

    ScpAction(None, localFilepath, Some(node), remoteFilePath)
    val res = SshAction(node, s"source $remoteFilePath", returnResult, ignoreError)
    SshAction(node, s"rm $remoteFilePath", returnResult = false, true)
    localTmpShellFile.delete()
    res
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
