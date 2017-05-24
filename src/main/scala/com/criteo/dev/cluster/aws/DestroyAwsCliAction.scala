package com.criteo.dev.cluster.aws

import com.criteo.dev.cluster.config.GlobalConfig
import com.criteo.dev.cluster.{CliAction, GeneralUtilities, Public}
import org.jclouds.compute.ComputeService
import org.jclouds.compute.domain.NodeMetadata.Status
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Destroy AWS clusters owned by this user.
  */
@Public object DestroyAwsCliAction extends CliAction[Unit] {

  override def command : String = "destroy-aws"

  override def usageArgs =
    List("cluster.id", Option("force"))

  override def help : String =
    "Destroys AWS cluster with given cluster.id." +
      "  Second option must be force if trying to destroy a cluster for another user."

  private val logger = LoggerFactory.getLogger(DestroyAwsCliAction.getClass)

  def applyInternal(args: List[String], config: GlobalConfig): Unit = {
    logger.info("Connecting to AWS to fetch nodes to destroy.")
    val conf = config.backCompat
    val instanceId = args(0)
    var result = {
      if (args.length == 2 && (args(1)).toLowerCase().equals("force")) {
        AwsUtilities.getCluster(conf, instanceId)
      } else {
        AwsUtilities.getUserCluster(conf, instanceId)
      }
    }
    if (result == null || result.master.getStatus().equals(Status.TERMINATED)) {
      logger.info("No clusters found matching criteria, or force not specified for deleting cluster of other users.")
      return
    }
    destroy(conf, List(result))
  }

  def destroy(conf: Map[String, String], clusters: Iterable[JcloudCluster]) = {
    val nodesToDestroy = clusters.flatMap(_.slaves) ++ clusters.map(_.master)

    val computeService = AwsUtilities.getComputeService(conf)
    logger.info(s"Destroying ${nodesToDestroy.size} nodes in parallel.")
    val futures = nodesToDestroy.filter(n => !(n.getStatus().equals(Status.TERMINATED))).map(n => {
      GeneralUtilities.getFuture {
        logger.info(s"Destroying instance ${n.getId()}")
        AwsUtilities.retryAwsAction(new RetryableDestroy(computeService, n.getId()))
        logger.info(s"Destroyed.")
      }
    })
    val result = Future.sequence(futures)
    Await.result(result, Duration.Inf)
  }
}


class RetryableDestroy(computeService : ComputeService, nodeid : String) extends Retryable[Any] {
  def action() : Unit = {
    computeService.destroyNode(nodeid)
  }
}
