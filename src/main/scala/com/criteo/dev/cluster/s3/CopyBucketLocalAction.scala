package com.criteo.dev.cluster.s3

import com.criteo.dev.cluster.config.GlobalConfig
import com.criteo.dev.cluster.copy.CopyAllAction
import com.criteo.dev.cluster.docker.{DockerConstants, DockerRunning, DockerUtilities}
import com.criteo.dev.cluster.{CliAction, NodeFactory, Public, SshAction}
import org.jclouds.blobstore.domain.{PageSet, StorageMetadata}
import org.jclouds.blobstore.options.ListContainerOptions

import scala.collection.JavaConverters._

/**
  * Copy everything from S3 to local cluster.
  */
@Public object CopyBucketLocalAction extends CliAction[Unit] {
  override def command: String = "copy-bucket-local"

  override def usageArgs: List[Any] = List("bucket-id", "container.id")

  override def help: String = "Attaches the given local docker cluster to Hive tables located in given S3 bucket.  " +
    "Any existing Hive metadata on cluster is not overriden, be aware to maintain consistency."


  override def applyInternal(args: List[String], config: GlobalConfig): Unit = {
    val bucketId = args(0)
    val dockerContainerId = args(1)
    val conf = config.backCompat

    //check if docker container id matches a running docker instance.
    val dockerMeta = DockerUtilities.getDockerContainerMetadata(
      DockerConstants.localClusterContainerLabel,
      Some(dockerContainerId))
    val runningDockerMeta = dockerMeta.filter(_.dockerState == DockerRunning)
    require(runningDockerMeta.length == 1, s"Cannot find running docker container with id $dockerContainerId")

    val target = NodeFactory.getDockerNode(config.target.local, runningDockerMeta(0))

    //copy the data over
    val folders = BucketUtilities.getAllFolders(conf, bucketId, target.nodeType)
    folders.foreach(f => SshAction(target, s"hadoop distcp $f /"))

    //create ddl
    RunS3DdlAction(target, bucketId, copiedLocally = true, conf)
  }
}
