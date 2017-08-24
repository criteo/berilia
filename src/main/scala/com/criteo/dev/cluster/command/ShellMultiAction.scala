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

  //to allow concurrency
  val filepath = s"${GeneralUtilities.getHomeDir}/${GeneralUtilities.getTempPrefix}.sh"

  def add(command: String): Unit = {
    commands.+=(command)
  }

  def run(returnResult: Boolean = false, ignoreError: Boolean = false): String = {
    val localTmpShellFile = new File(filepath)
    ShellAction(s"rm $filepath", returnResult = false, true)

    //Write a temp shell script
    val writer = new PrintWriter(localTmpShellFile)
    commands.foreach(s => writer.write(s"$s\n"))
    writer.close

    localTmpShellFile.setExecutable(true)
    localTmpShellFile.setReadable(true)
    localTmpShellFile.deleteOnExit()

    commands.foreach(s => logger.info(s))
    val res = ShellAction(filepath, returnResult, ignoreError)
    localTmpShellFile.delete()
    res
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
