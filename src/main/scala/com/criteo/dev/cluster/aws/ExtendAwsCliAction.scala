package com.criteo.dev.cluster.aws

import com.criteo.dev.cluster.{CliAction, Public}
import org.jclouds.compute.domain.NodeMetadata.Status
import org.scala_tools.time.Imports._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * Extending the expire time of a cluster.
  */
@Public object ExtendAwsCliAction extends CliAction[Unit] {

  override def command: String = "extend-aws"

  override def usageArgs: List[Any] = List("cluster.id")

  override def help: String = "Extends expiry time of cluster with given cluster.id"

  private val logger = LoggerFactory.getLogger(ExtendAwsCliAction.getClass)

  override def applyInternal(args: List[String], conf: Map[String, String]): Unit = {
    logger.info(s"Updating node.  Searching in group ${AwsConstants.groupTag} owned by current user: ${System.getenv("USER")}")

    val instanceId = args(0)
    val cluster = AwsUtilities.getUserCluster(conf, instanceId)
    if (!cluster.master.getStatus().equals(Status.RUNNING)) {
      throw new IllegalArgumentException("Cannot extend time of a AWS cluster that is not running.")
    }
    extend(conf, cluster)
  }

  /**
    * Extends expiration time of node
    *
    * @param conf
    * @param cluster
    * @param reset
    */
  def extend(conf: Map[String, String], cluster: JcloudCluster,
             reset: Boolean = false) = {

    val region = AwsUtilities.getAwsProp(conf, AwsConstants.region)
    val tagApi = AwsUtilities.getEc2Api(conf).getTagApiForRegion(region).get()

    val master = cluster.master
    val masterMetadata = master.getUserMetadata
    val masterId = AwsUtilities.stripRegion(conf, master.getId)

    logger.info(s"Updating expiration time for master $masterId")
    extendTimeMetadata(masterMetadata, reset)
    tagApi.applyToResources(masterMetadata, List(masterId).asJava)
  }

  def extendTimeMetadata(metadata: java.util.Map[String, String], reset : Boolean) = {
    var currentExpireTime = metadata.get(AwsConstants.expireTime)
    if (reset || (currentExpireTime == null)) {
      currentExpireTime = AwsUtilities.getCurrentTime()
    }
    logger.info(s"Current expiration time is : $currentExpireTime (UTC)")
    val newExpireTime = AwsUtilities.updateExpireTime(currentExpireTime)

    val limitTime = (DateTime.now + AwsConstants.twiceExtensionTime).withZone(DateTimeZone.UTC)
    if (AwsUtilities.stringToDt(newExpireTime).isAfter(limitTime)) {
       throw new IllegalArgumentException(s"Cannot update time to $newExpireTime (UTC)" +
         s", as it is must be less than: ${AwsUtilities.dtToString(limitTime)} (UTC)")
    }
    metadata.put(AwsConstants.expireTime, newExpireTime)
    logger.info(s"Updating node to new expiration time : $newExpireTime (UTC)")
  }
}
