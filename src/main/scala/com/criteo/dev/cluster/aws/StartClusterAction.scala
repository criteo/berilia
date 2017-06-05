package com.criteo.dev.cluster.aws

import com.criteo.dev.cluster.aws.AwsUtilities.NodeRole
import com.criteo.dev.cluster.config.AWSConfig
import com.criteo.dev.cluster.{GeneralUtilities, NodeFactory, StartServiceAction}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Sets up a cluster in parallel.
  *
  * Runs SetupNodeAction for individual nodes of a cluster, with different services
  * started whether its a master or a slave.
  */
object StartClusterAction {

  private val logger = LoggerFactory.getLogger(StartClusterAction.getClass)

  def apply(conf: AWSConfig, clusters: Iterable[JcloudCluster]) = {

    clusters.foreach(c => {
      logger.info(s"Starting services on master ${c.master.getId}")
      val masterNode = NodeFactory.getAwsNode(conf, c.master)
      StartServiceAction(masterNode, NodeRole.Master)
      logger.info(s"Successfully started services on master ${c.master.getId}")

      logger.info(s"Starting services on ${c.slaves.size} in parallel.")
      val setupSlaves = c.slaves.map(s => GeneralUtilities.getFuture {
        logger.info(s"Starting service on slave ${s.getId}")
        val slaveNode = NodeFactory.getAwsNode(conf, s)
        StartServiceAction(slaveNode, NodeRole.Slave)
        logger.info(s"Successfully started service on slave ${s.getId}")
      })

      val aggSetupSlaveFutures = Future.sequence(setupSlaves)
      Await.result(aggSetupSlaveFutures, Duration.Inf)
    })
  }
}
