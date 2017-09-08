package com.criteo.dev.cluster.aws

import com.criteo.dev.cluster.config.GlobalConfig
import com.criteo.dev.cluster.{CliAction, Public}
import org.jclouds.compute.domain.NodeMetadata.Status


/**
  * Restart all Hadoop services on all nodes of the cluster.  It can be a brute-force way to make sure all services are up.
  */
@Public object RestartServicesCliAction extends CliAction[Unit] {
  override def command: String = "restart-services-aws"

  override def usageArgs: List[Any] = List("cluster.id")

  override def help: String = "Restart all Hadoop services on all nodes in given cluster."

  override def applyInternal(args: List[String], config: GlobalConfig): Unit = {
    val id = args(0)
    val conf = config.backCompat
    val cluster = AwsUtilities.getUserCluster(conf, id)
    require (cluster.master.getStatus().equals(Status.RUNNING), "No clusters found in RUNNING state.")
    StartClusterAction(config.app.aws, List(cluster))
  }
}
