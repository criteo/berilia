package com.criteo.dev.cluster.s3

import com.criteo.dev.cluster.aws.AwsUtilities
import com.criteo.dev.cluster.config.GlobalConfig
import com.criteo.dev.cluster.docker.{DockerConstants, DockerUtilities}
import com.criteo.dev.cluster.{CliAction, NodeFactory, Public, SshAction}
import org.jclouds.blobstore.options.ListContainerOptions
import org.jclouds.compute.domain.NodeMetadata.Status

import scala.collection.JavaConverters._

/**
  * Copy everything from S3 to AWS cluster.
  */
@Public object CopyBucketAwsAction extends CliAction[Unit] {
  override def command: String = "copy-bucket-aws"

  override def usageArgs: List[Any] = List("bucket-id", "container.id")

  override def help: String = "Copies all data from given S3 bucket to the local AWS cluster."


  override def applyInternal(args: List[String], config: GlobalConfig): Unit = {
    val bucketId = args(0)
    val instanceId = args(1)
    val conf = config.backCompat
    //check if docker container id matches a running docker instance.
    val cluster = AwsUtilities.getUserCluster(conf, instanceId)
    val master = cluster.master

    require(master.getStatus().equals(Status.RUNNING), "No clusters found in RUNNING state matching criteria.")

    val target = NodeFactory.getAwsNode(conf, master)

    //copy the data over
    val folders = BucketUtilities.getAllFolders(conf, bucketId, target.nodeType)
    folders.foreach(f => SshAction(target, s"hadoop distcp $f /"))

    //create ddl
    RunS3DdlAction(target, bucketId, copiedLocally = true, conf)
  }
}
