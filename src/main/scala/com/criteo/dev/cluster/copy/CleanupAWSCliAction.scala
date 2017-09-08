package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.aws.AwsUtilities
import com.criteo.dev.cluster.config.GlobalConfig
import com.criteo.dev.cluster.{CliAction, NodeFactory, Public}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

@Public object CleanupAWSCliAction extends CliAction[Unit] {
  private val logger = LoggerFactory.getLogger(this.getClass)

  override def command = "cleanup-aws"

  override def usageArgs = List("cluster.id")

  override def help = "Cleanup the Hive database of the cluster, the tables to be removed are specified in the source config"

  override def applyInternal(args: List[String], config: GlobalConfig): Unit = {
    val instanceId = args(0)
    val cluster = AwsUtilities.getCluster(config.backCompat, instanceId)
    val target = NodeFactory.getAwsNode(config.app.aws, cluster.master)

    CleanupHiveAction(config, target) match {
      case Success(CleanResult(hive, hdfs)) =>
        logger.info("Cleanup has succeeded")
        logger.info("Hive:")
        logger.info(hive)
        logger.info("HDFS:")
        logger.info(hdfs)
      case Failure(_) =>
        logger.info("Cleanup has failed")
    }
  }
}
