package com.criteo.dev.cluster.aws

import com.criteo.dev.cluster.config.GlobalConfig
import com.criteo.dev.cluster.{CliAction, Public}
import org.jclouds.compute.domain.NodeMetadata
import org.jclouds.compute.domain.NodeMetadata.Status
import org.joda.time.DateTime
import org.scala_tools.time.Imports._
import org.slf4j.LoggerFactory

/**
  * Purge expired AWS clusters.
  */
object PurgeAwsCliAction extends CliAction[Unit] {

  override def command: String = "purge"

  override def usageArgs: List[Any] = List()

  override def help: String = "Purge all expired Running clusters."

  override def isHidden = true

  private val logger = LoggerFactory.getLogger(PurgeAwsCliAction.getClass)

  override def applyInternal(args: List[String], config: GlobalConfig): Unit = {
    val currentTime = DateTime.now(DateTimeZone.UTC)
    val conf = config.backCompat

    logger.info(s"Running purge, current time is ${AwsUtilities.dtToString(currentTime)} (UTC)")
    val results = AwsUtilities.getAllClusters(conf).filter(c => c.master.getStatus.equals(Status.RUNNING))
    val toPurge = results.filter(c => isPurgable(c.master, currentTime))

    toPurge.foreach(u => {
      val nodeText = AwsUtilities.nodeInformation(conf, u.master)
      logger.info(s"Found node of user : ${AwsUtilities.getUser(u.master)}")
      logger.info(nodeText)
    })

    DestroyAwsCliAction.destroy(conf, toPurge)
  }

  def isPurgable(node: NodeMetadata, dt: DateTime) : Boolean = {
    val userMetadata = node.getUserMetadata
    val expireTime = userMetadata.get(AwsConstants.expireTime)
    if (expireTime == null) {
      //kill nodes without expire time
      return true
    }

    try {
      val date = AwsUtilities.stringToDt(expireTime)
      return date.isBefore(dt)
    } catch {
      //if expire time is corrupt, kill the node.
      case e: IllegalArgumentException => return true
    }
  }
}
