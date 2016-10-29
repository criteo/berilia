package com.criteo.dev.cluster

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

  def apply(args: List[String], conf: Map[String, String]) : T = {
    verify(args)
    val finalConf = CommandRegistry.fillSystemConfs(conf, this)
    applyInternal(args, finalConf)
  }

  def applyInternal(args: List[String], conf: Map[String, String]) : T
}


