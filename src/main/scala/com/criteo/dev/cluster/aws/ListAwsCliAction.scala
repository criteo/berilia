package com.criteo.dev.cluster.aws


import org.slf4j.Logger
import com.criteo.dev.cluster.{CliAction, Public}
import org.jclouds.compute.domain.NodeMetadata
import org.slf4j.LoggerFactory

/**
  * Lists AWS clusters of this user.
  */
@Public
object ListAwsCliAction extends CliAction[List[AwsCluster]] {

  override def command : String = "list-aws"

  override def usageArgs = List()

  override def help() = "Lists all clusters owned by this user, and details of nodes within the cluster."

  private val logger = LoggerFactory.getLogger(ListAwsCliAction.getClass)

  def applyInternal(args: List[String], conf: Map[String, String]): List[AwsCluster] = {
    logger.info(s"Listing clusters with group ${AwsConstants.groupTag} owned by current user: ${System.getenv("USER")}")
    AwsUtilities.retryAwsAction(new RetryableList(logger, conf))
  }
}


class RetryableList(logger: Logger, conf: Map[String, String]) extends Retryable[List[AwsCluster]] {
  def action() : List[AwsCluster] = {
    val clusters = AwsUtilities.getUserClusters(conf)

    println
    clusters.foreach(c => {
      AwsUtilities.printClusterInfo(conf, c)
      println
    })

    clusters.map(c => AwsUtilities.getAwsCluster(conf, c)).toList
  }
}
