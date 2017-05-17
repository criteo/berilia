package com.criteo.dev.cluster.aws


import com.criteo.dev.cluster._
import com.criteo.dev.cluster.aws.AwsUtilities.NodeRole
import com.criteo.dev.cluster.config.GlobalConfig
import com.google.common.util.concurrent.Futures
import org.jclouds.compute.ComputeService
import org.jclouds.compute.domain.NodeMetadata.Status
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Starts an AWS clusters.
  */
@Public object StartAwsCliAction extends CliAction[Unit] {

  override def command : String = "start-aws"

  override def usageArgs = List(Option("cluster.id"))

  override def help() = "Starting a stopped cluster with given cluster.id.  " +
    "If no cluster.id is given, start all stopped clusters owned by this user. " +
    "Note that AWS assigns new public ips for restarted nodes in the cluster.  " +
    "Expiration countdown is extended for restarted clusters."

  private val logger = LoggerFactory.getLogger(StartAwsCliAction.getClass)

  def applyInternal(args: List[String], config: GlobalConfig): Unit = {
    logger.info("Connecting to AWS to fetch nodes to start.")
    val conf = config.backCompat
    var clusters = getClusters(args, conf)
    clusters = clusters.filter(u => u.master.getStatus().equals(Status.SUSPENDED))
    if (clusters.size == 0) {
      logger.info("No clusters found matching criteria.")
    }

    //start nodes in parallel
    val nodesToStart = clusters.flatMap(_.slaves) ++ clusters.map(_.master)
    logger.info(s"Starting ${nodesToStart.size} nodes in parallel.")
    val computeService = AwsUtilities.getComputeService(conf)
    val startFutures = nodesToStart.filter(u => u.getStatus().equals(Status.SUSPENDED))
      .map(u => GeneralUtilities.getFuture {
        val shortId = AwsUtilities.stripRegion(conf, u.getId)
        logger.info(s"Starting instance $shortId")
        AwsUtilities.retryAwsAction(new RetryableStart(computeService, u.getId()))
      }
    )

    val aggStartFuture = Future.sequence(startFutures)
    Await.result(aggStartFuture, Duration.Inf)

    //lookup nodes and reconfigure.
    //Sometimes /etc/hosts gets regenerated on new instances, sometimes they do not.
    val startedClusters = clusters.map(_.master.getId).toSet
    val newClusters = getClusters(args, conf).filter(c => startedClusters.contains(c.master.getId))
    ConfigureHostsAction(conf, newClusters)

    newClusters.foreach(c => ExtendAwsCliAction.extend(conf, c, reset=true))

    logger.info("Restarting services in parallel.")
    StartClusterAction(conf, newClusters)

    //print out all the infos.
    newClusters.foreach(c => AwsUtilities.printClusterInfo(conf, c))
  }

  def getClusters(args: List[String], conf: Map[String, String]): Iterable[JcloudCluster] = {
    if (args.length == 1) {
      //instance id is optional
      val instanceId = args(0)
      Set(AwsUtilities.getUserCluster(conf, instanceId))
    } else {
      AwsUtilities.getUserClusters(conf)
    }
  }
}

class RetryableStart(computeService: ComputeService, nodeid: String) extends Retryable[Any] {
  def action: Unit = {
    computeService.resumeNode(nodeid)
  }
}
