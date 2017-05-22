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
    List(Option("cluster.id"))

  override def help : String =
    "Destroys AWS cluster with given cluster.id." +
      "  If no cluster.id is given, destroy all clusters for this user."

  private val logger = LoggerFactory.getLogger(DestroyAwsCliAction.getClass)

  def applyInternal(args: List[String], config: GlobalConfig): Unit = {
    logger.info("Connecting to AWS to fetch nodes to destroy.")
    val conf = config.backCompat
    var results = {
      if (args.length == 1) {
        //instance id is optional
        val instanceId = args(0)
        Set(AwsUtilities.getUserCluster(conf, instanceId))
      } else {
        AwsUtilities.getUserClusters(conf)
      }
    }
    results = results.filter(u => !u.master.getStatus().equals(Status.TERMINATED))
    if (results.size == 0) {
      logger.info("No clusters found matching criteria.")
    }
    destroy(conf, results)
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
