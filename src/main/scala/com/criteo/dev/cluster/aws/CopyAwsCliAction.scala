package com.criteo.dev.cluster.aws

import com.criteo.dev.cluster._
import com.criteo.dev.cluster.copy.{CreateMetadataHiveAction$, _}
import org.jclouds.compute.domain.NodeMetadata.Status
import org.slf4j.LoggerFactory

/**
  * Copies from source to AWS cluster.
  */
@Public object CopyAwsCliAction extends CliAction[Unit] {

  private val logger = LoggerFactory.getLogger(CopyAwsCliAction.getClass)

  override def command : String = "copy-aws"

  override def usageArgs: List[Any] = List("cluster.id")

  override def help: String = "Copies sample data from gateway to AWS cluster identified by cluster.id"

  override def applyInternal(args: List[String], conf: Map[String, String]): Unit = {
    //find the ip
    val instanceId = args(0)
    val cluster = AwsUtilities.getUserCluster(conf, instanceId)
    val master = cluster.master
    require(master.getStatus().equals(Status.RUNNING), "No clusters found in RUNNING state matching criteria.")
    val ip = AwsUtilities.ipAddress(master)

    val newConf = collection.mutable.Map(conf.toArray:_*)
    newConf.+= (AwsConstants.getAddressFull -> ip) //add target.address
    CopyAllAction(newConf.toMap)

    logger.info(s"Successfully copied to cluster: ${cluster.master.getId}")
    AwsUtilities.printClusterInfo(conf, cluster)
  }
}
