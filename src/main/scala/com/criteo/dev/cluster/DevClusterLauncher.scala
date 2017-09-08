package com.criteo.dev.cluster

import java.io.{File, FileNotFoundException}
import java.net.URL

import com.criteo.dev.cluster.config.{ConfigLoader, GlobalConfig}

/**
  * Main entry point to various actions on the AWS-hosted development clusters for the current user.
  *
  * See application.properties for example of how to configure source, target, and sample tables.
  */

object DevClusterLauncher {


  def main(args: Array[String]) {
    if (args.length == 0) {
      printHelp
      System.exit(1)
    }

    if (System.getenv("USER") == null) {
      println("Required variable USER is not set.")
      System.exit(1)
    }

    val commandMap = CommandRegistry.getCommandMap
    val commandString = args(0).trim()

    val command = commandMap.get(commandString)

    if (command isEmpty) {
      println(s"Invalid command: [$commandString]. Following commands are valid.")
      printHelp
      System.exit(1)
    } else {
      try {
        val argList = args.toList.drop(1)
        val realArgs = argList.filterNot(_.startsWith("--"))
        val conf = ConfigLoader(
          getOption(args, "config").map(getFileURL(_)).getOrElse(getFileURL("app.conf")),
          getOption(args, "checkpoint").map(getFileURL(_))
        ).value
        command.get.apply(realArgs, conf)
      } catch {
        case e: Exception => {
          e.printStackTrace()
          System.exit(1)
        }
      }
    }
    System.exit(0)
  }

  def getFileURL(path: String): URL = {
    val file = new File(path)
    if (file.exists)
      file.toURI.toURL
    else
      throw new FileNotFoundException(s"$path does not exist")
  }

  def getOption(args: Array[String], argName: String): Option[String] = args
    .find(_.startsWith(s"--$argName"))
    .flatMap(_.split("=").drop(1).headOption)


  def printHelp(): Unit = {
    println("This tool provides utilities for creating and managing AWS dev instances, " +
      "and utilities such as copying data from gateway, and configuring gateway on local " +
      "machine to the cluster.  Use the following commands.\n")
    CommandRegistry.getCommands.foreach(
      cc => {
        println(s"\033[1m${cc.name} commands\033[0m")
        println()
        cc.actions.filter(_.isHidden == false).foreach(c => {
          println(s"* ${c.command}")
          c.printHelp
          println()
        })
      })
  }
}


object HelpAction extends CliAction[Unit] {
  override def command: String = "help"

  override def usageArgs: List[Any] = List()

  override def help: String = "Gets help"

  override def applyInternal(args: List[String], config: GlobalConfig): Unit = {
    DevClusterLauncher.printHelp
  }
}
