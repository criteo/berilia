package com.criteo.dev.cluster.aws


import com.criteo.dev.cluster.{CliAction, GeneralUtilities, Public}
import org.jclouds.compute.ComputeService
import org.jclouds.compute.domain.NodeMetadata.Status
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Stops an AWS cluster.
  */
@Public object StopAwsCliAction extends CliAction[Unit] {

  override def command : String = "stop-aws"

  override def usageArgs = List(Option("cluster.id"))

  override def help: String = "Stopping a running cluster with given cluster.id.  " +
    "If no cluster.id is given, stop all running clusters owned by this user.  " +
    "Stopping a cluster prevents it from being purged due to expiration."

  private val logger = LoggerFactory.getLogger(StopAwsCliAction.getClass)

  def applyInternal(args: List[String], conf: Map[String, String]): Unit = {
    var clusters = {
      if (args.length == 1) {
        //instance id is optional
        val instanceId = args(0)
        Set(AwsUtilities.getUserCluster(conf, instanceId))
      } else {
        AwsUtilities.getUserClusters(conf)
      }
    }
    clusters = clusters.filter(u => u.master.getStatus().equals(Status.RUNNING))
    if (clusters.size == 0) {
      logger.info("No clusters found matching criteria.")
      return
    }
    val nodesToStop = clusters.flatMap(_.slaves) ++ clusters.map(_.master)

    val computeService = AwsUtilities.getComputeService(conf)
    logger.info(s"Stopping ${nodesToStop.size} nodes in parallel")
    val futures = nodesToStop.filter(n => (n.getStatus().equals(Status.RUNNING))).map(u => GeneralUtilities.getFuture{
      logger.info(s"Stopping instance ${AwsUtilities.stripRegion(conf, u.getId())}")
      AwsUtilities.retryAwsAction(new RetryableStop(computeService, u.getId()))
      logger.info(s"Stopped.")
    })
    val aggFutures = Future.sequence(futures)
    Await.result(aggFutures, Duration.Inf)
  }
}


class RetryableStop(computeService: ComputeService, nodeid: String) extends Retryable[Any] {
  def action: Unit = {
    computeService.suspendNode(nodeid)
  }
}
