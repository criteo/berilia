package com.criteo.dev.cluster

import org.slf4j.LoggerFactory

import scala.sys.process.{Process, ProcessLogger}

/**
  * For now, only supports copy from local to remote.  Can be enhanced later to support reverse.
  *
  * Rsync can copy between two servers, but it is a pain and done via SSH tunnels.
  */
@Public
object RsyncAction {
  private val logger = LoggerFactory.getLogger(ScpAction.getClass)
  private val processLogger = ProcessLogger(
    (o: String) => logger.info(o),
    (e: String) => logger.error(e))

  def apply(srcPath: String, targetN: Node, targetPath: String, sudo: Boolean = false) : Unit = {
    var command = scala.collection.mutable.ListBuffer[String]()
    command += "rsync"
    command += "-rvvz"
    command += "-e"

    val sshStr = new StringBuilder("ssh -o StrictHostKeyChecking=no ")
    val targetKey = targetN.key
    val targetUser = targetN.user
    val targetIp = targetN.ip
    val targetPort = targetN.port

    if (targetKey.isDefined) {
      sshStr.append(s"-i ${targetKey.get}")
    }

    if (targetPort.isDefined) {
      sshStr.append(s"-P ${targetPort.get}")
    }
    command += sshStr.toString

    if (sudo) {
      command += "--rsync-path=sudo rsync"
    }

    command += srcPath

    val targetPathFull = new StringBuilder()
    if (targetUser.isDefined) {
      targetPathFull.append(s"${targetUser.get}@")
    }
    targetPathFull.append(s"$targetIp:")
    targetPathFull.append(targetPath)
    command += targetPathFull.toString

    val p = DevClusterProcess.processSeq(command)
    GeneralUtilities.checkStatus(p.!(processLogger))
  }
}

