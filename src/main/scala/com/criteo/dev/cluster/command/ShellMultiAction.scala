package com.criteo.dev.cluster.command

import java.io.{File, PrintWriter}

import com.criteo.dev.cluster.{GeneralUtilities, Public}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.sys.process.ProcessLogger

/**
  * Collects a bunch of commands into a shell script and executes it.
  */
@Public
case class ShellMultiAction() extends MultiAction {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val commands = new ListBuffer[String]
  private val processLogger = ProcessLogger(
    (e: String) => logger.info("err " + e))

  //to allow concurrency
  val localTmpShell = s"${GeneralUtilities.getTempDir}/tmp.sh"

  def add(command: String): Unit = {
    commands.+=(command)
  }

  def run(returnResult: Boolean = false, ignoreError: Boolean = false): String = {
    //cleanup tmp files, ignore errors in these commands.
    GeneralUtilities.prepareTempDir
    val localTmpShellFile = new File(s"${GeneralUtilities.getHomeDir}/$localTmpShell")
    ShellAction("rm " + localTmpShell, returnResult = false, true)

    //Write a temp shell script
    val writer = new PrintWriter(localTmpShellFile)
    commands.foreach(s => writer.write(s"$s\n"))
    writer.close

    localTmpShellFile.setExecutable(true)
    localTmpShellFile.setReadable(true)
    localTmpShellFile.deleteOnExit()

    commands.foreach(s => logger.info(s))
    ShellAction(localTmpShell, returnResult, ignoreError)
  }
}

object ShellMultiAction {
  def apply(
             commands: List[String],
             returnResult: Boolean = false,
             ignoreError: Boolean = false
           ): String = {
    val action = new ShellMultiAction()
    commands.foreach(action.add)
    action.run(returnResult, ignoreError)
  }
}