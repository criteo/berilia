package com.criteo.dev.cluster

import com.criteo.dev.cluster.aws._
import com.criteo.dev.cluster.docker._
import org.slf4j.LoggerFactory

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
        val profiles = getProfiles(argList)
        val realArgs = argList.filterNot(profilePred)
        val conf = ConfigManager.load(profiles)
        command.get.apply(realArgs, conf)
      } catch {
        case e:Exception => {
          e.printStackTrace()
          System.exit(1)
        }
      }
    }
    System.exit(0)
  }


  def printHelp() : Unit = {
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

  def profilePred = (s: String) => s.startsWith("-P")

  /**
    * Inspired by maven profiles.
    *
    *  Return profiles given by flag -P$profile.  Also handles comma separated profiles list like
    * -P$p1,$p2.
    */
  def getProfiles(args: List[String]): List[String] = {
    val profileArgs = args.filter(profilePred)
    val strippedProfiles = profileArgs.map(_.stripPrefix("-P"))
    strippedProfiles.flatMap(_.split(","))
  }
}


object HelpAction extends CliAction[Unit] {
  override def command: String = "help"

  override def usageArgs: List[Any] = List()

  override def help: String = "Gets help"

  override def applyInternal(args: List[String], conf: Map[String, String]): Unit = {
    DevClusterLauncher.printHelp
  }
}
