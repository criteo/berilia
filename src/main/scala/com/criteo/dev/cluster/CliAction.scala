package com.criteo.dev.cluster

import com.criteo.dev.cluster.config.GlobalConfig

/**
  * Generalizes node actions for the command line.
  */
abstract class CliAction[+T] {

  def command: String

  def usageArgs: List[Any]

  def isHidden = false

  var commandCategory: CliActionCategory = null

  def usage : String = {
    val usageString = usageArgs.map(u => {
      u match {
        case s: String => s"[$s]"
        case o: Option[String] => s"[(Optional) ${o.get}]"
      }
    }).mkString(" ")
    s"dev-cluster $command $usageString"
  }

  def help : String

  def printHelp = {
    println(s"$help")
    println(s"Usage: $usage")
  }

  def verify(args: List[String]) = {
    val reqArgs = usageArgs.filter(u => !u.isInstanceOf[Option[Any]])
    if ((args.length < reqArgs.length) || (args.length > usageArgs.length)) {
      printHelp
      System.exit(1)
    }
  }

  def apply(args: List[String], config: GlobalConfig) : T = {
    verify(args)
    val finalConf = CommandRegistry.fillSystemConfs(config.backCompat, this)
    applyInternal(args, config.copy(backCompat = finalConf))
  }

  def applyInternal(args: List[String], conf: GlobalConfig) : T
}


