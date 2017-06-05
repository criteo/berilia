package com.criteo.dev.cluster.aws

import java.io.File
import java.net.{URI, URL}

import com.criteo.dev.cluster.config.GlobalConfig
import com.criteo.dev.cluster._
import org.jclouds.compute.domain.NodeMetadata.Status
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import sys.process._

/**
  * Copies a file from source to destination path to all nodes of a given cluster (if target directory exists).
  */
@Public object CopyJarAwsCliAction extends CliAction[Unit] {
  override def command: String = "copy-jar-aws"

  override def usageArgs: List[Any] = List("instance.id", "source", "destination")

  override def help: String = "Copies a file from source to destination path to all nodes of a given cluster (if target directory exists)."

  private val logger = LoggerFactory.getLogger(CopyJarAwsCliAction.getClass)

  override def applyInternal(args: List[String], conf: GlobalConfig): Unit = {
    val instanceId = args(0)
    var cluster = AwsUtilities.getUserCluster(conf.backCompat, instanceId)

    if (!cluster.master.getStatus().equals(Status.RUNNING)) {
      logger.info("No running clusters found matching criteria.")
    }

    val source = args(1)
    val target = args(2)
    val sourceUri = new URI(source)
    val targetFile = new File(target)

    GeneralUtilities.prepareTempDir
    val sourceFile = sourceUri.getScheme().toLowerCase() match {
      case "http" => {
        val path = s"${GeneralUtilities.getTempDir()}/${targetFile.getName}"
        DevClusterProcess.process(s"curl -o $path $source").!!
        path
      }
      //only localhost supported
      case "file" => sourceUri.getPath()
      case _ => throw new IllegalArgumentException("Only http and file supported for sources for now.")
    }

    //copy over files in parallel
    val nodesToCopy = cluster.slaves ++ Set(cluster.master)
    logger.info(s"Copying to ${nodesToCopy.size} nodes in parallel.")
    val copyFutures = nodesToCopy.map(u => GeneralUtilities.getFuture {
      val targetN = NodeFactory.getAwsNode(conf.target.aws, u)
      val role = if (AwsUtilities.isSlave(u)) "Slave" else "Master"
      try {
        RsyncAction(
          srcPath = sourceFile,
          targetN = targetN,
          targetPath = target,
          sudo = true)
        s"$role Node ${u.getId()} with ${targetN.ip}: Copy successful."
      } catch {
        case e : Exception => s"$role Node ${u.getId()} with ${targetN.ip}: Copy Failed.  This is normal if the given directory does not exist on the node." +
          s"  If not expected, check the directory location and try again."
      }
    })

    val aggCopyFuture = Future.sequence(copyFutures)
    val result = Await.result(aggCopyFuture, Duration.Inf)
    result.foreach(r => logger.info(r))
    GeneralUtilities.cleanupTempDir
  }
}
