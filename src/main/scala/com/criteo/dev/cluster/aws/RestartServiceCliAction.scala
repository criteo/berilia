package com.criteo.dev.cluster.aws

import com.criteo.dev.cluster.{CliAction, Public}
import org.jclouds.compute.domain.NodeMetadata.Status


/**
  * Restart all Hadoop services on all nodes of the cluster.  It can be a brute-force way to make sure all services are up.
  */
@Public object RestartServiceCliAction extends CliAction[Unit] {
  override def command: String = "restart-services-aws"

  override def usageArgs: List[Any] = List("cluster.id")

  override def help: String = "Restart all Hadoop services on all nodes in given cluster."

  override def applyInternal(args: List[String], conf: Map[String, String]): Unit = {
    val id = args(0)
    val cluster = AwsUtilities.getUserCluster(conf, id)
    require (cluster.master.getStatus().equals(Status.RUNNING), "No clusters found in RUNNING state.")
    StartClusterAction(conf, List(cluster))
  }
}
