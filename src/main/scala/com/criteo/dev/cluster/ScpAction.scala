package com.criteo.dev.cluster

import org.slf4j.LoggerFactory

import scala.sys.process._

@Public
object ScpAction {
  private val logger = LoggerFactory.getLogger(ScpAction.getClass)
  private val processLogger = ProcessLogger(
    (o: String) => logger.info(o),
    (e: String) => logger.error(e))

  /**
    * @param sourceN if null, means local node
    * @param srcPath source path
    * @param targetN if null, means local node
    * @param targetPath target path
    */
  def apply(sourceN: Option[Node], srcPath: String, targetN: Option[Node], targetPath: String) : Unit = {
    val sourceKey = if (sourceN.isDefined) sourceN.get.key else None
    val sourceUser = if (sourceN.isDefined) sourceN.get.user else None
    val sourceIp = if (sourceN.isDefined) Some(sourceN.get.ip) else None
    val sourcePort = if (sourceN.isDefined) sourceN.get.port else None

    val targetKey = if (targetN.isDefined) targetN.get.key else None
    val targetUser = if (targetN.isDefined) targetN.get.user else None
    val targetIp = if (targetN.isDefined) Some(targetN.get.ip) else None
    val targetPort = if (targetN.isDefined) targetN.get.port else None


    val sb = new StringBuilder("scp -o StrictHostKeyChecking=no -r ")
    if (sourceIp.isDefined && targetIp.isDefined) {
      sb.append("-3 ")
    }

    if (sourceKey.isDefined || targetKey.isDefined) {
      sb.append(s"-i ")
      if (sourceKey.isDefined) {
        sb.append(s"${sourceKey.get} ")
      }
      if (targetKey.isDefined) {
        sb.append(s"${targetKey.get} ")
      }
    }

    if (sourcePort.isDefined || targetPort.isDefined) {
      sb.append(s"-P ")
      if (sourcePort.isDefined) {
        sb.append(s"${sourcePort.get} ")
      }
      if (targetPort.isDefined) {
        sb.append(s"${targetPort.get} ")
      }
    }

    if (sourceUser.isDefined) {
      sb.append(s"${sourceUser.get}@")
    }
    if (sourceIp.isDefined) {
      sb.append(s"${sourceIp.get}:")
    }
    sb.append(s"$srcPath ")

    if (targetUser.isDefined) {
      sb.append(s"${targetUser.get}@")
    }
    if (targetIp.isDefined) {
      sb.append(s"${targetIp.get}:")
    }
    sb.append(targetPath)
    val command = sb.toString
    val commands = command.split("\\s+")
    val p = DevClusterProcess.processSeq(commands)
    p.!!(processLogger)
  }
}
