package com.criteo.dev.cluster.s3

import com.criteo.dev.cluster.{CliAction, NodeFactory, Public}
import com.criteo.dev.cluster.aws.{AwsConstants, AwsUtilities}
import com.criteo.dev.cluster.config.GlobalConfig
import org.jclouds.compute.domain.NodeMetadata.Status
import org.slf4j.LoggerFactory

/**
  * Points a AWS-hosted dev cluster to be able to run queries against tables in a given s3 bucket.
  */
@Public object AttachBucketAwsCliAction extends CliAction[Unit] {

  private val logger = LoggerFactory.getLogger(AttachBucketAwsCliAction.getClass)

  override def command: String = "attach-bucket-aws"

  override def usageArgs: List[Any] = List("bucket-id", "instance.id")

  override def help: String = "Attaches the given AWS cluster to Hive tables located in given S3 bucket.  " +
    "Any existing Hive metadata on cluster is not overriden, be aware to maintain consistency."

  override def applyInternal(args: List[String], config: GlobalConfig): Unit = {
    val bucketId = args(0)
    val instanceId = args(1)
    val conf = config.backCompat

    //check if docker container id matches a running docker instance.
    val cluster = AwsUtilities.getUserCluster(conf, instanceId)
    val master = cluster.master

    require(master.getStatus().equals(Status.RUNNING), "No clusters found in RUNNING state matching criteria.")

    val node = NodeFactory.getAwsNode(config.target.aws, master)
    RunS3DdlAction(node, bucketId, copiedLocally = false, conf)

    AwsUtilities.printClusterInfo(conf, cluster)
  }
}
