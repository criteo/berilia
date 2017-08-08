package com.criteo.dev.cluster.aws

import com.criteo.dev.cluster.aws.AwsUtilities.NodeRole
import com.criteo.dev.cluster._
import com.criteo.dev.cluster.command.{ScpAction, SshAction}
import com.criteo.dev.cluster.config.{AWSConfig, TargetConfig}
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Installs hadoop on a given cluster, in parallel.
  */
object InstallHadoopAction {

  private val logger = LoggerFactory.getLogger(InstallHadoopAction.getClass)

  def apply(config: TargetConfig, cluster: JcloudCluster) = {
    logger.info(s"Installing CDH on ${cluster.size} nodes in parallel.")
    val hadoopVersion = config.common.hadoopVersion
    val masterNode = NodeFactory.getAwsNode(config.aws, cluster.master)

    val installMaster = GeneralUtilities.getFuture {
      val setupMaster = AwsUtilities.getOsSetupScript(config.common, NodeRole.Master)
      logger.info(s"Running $setupMaster on master ${masterNode.ip}")
      ScpAction(
        sourceN = None,
        srcPath = s"${GeneralConstants.setupScriptDir}/$setupMaster",
        targetN = Some(masterNode),
        targetPath = "~/setup.sh")

      //script will check if the specified hadoop version is valid.
      SshAction(masterNode, s"source setup.sh $hadoopVersion")
      SshAction(masterNode, "rm setup.sh")
      CopyHiveJarAction(config, cluster.master, NodeRole.Master)
      "" //otherwise there is NPE
    }

    val installSlaves = cluster.slaves.map(slaveMeta => {
      GeneralUtilities.getFuture {
        val slaveNode = NodeFactory.getAwsNode(config.aws, slaveMeta)
        val slaveSetup = AwsUtilities.getOsSetupScript(config.common, NodeRole.Slave)
        logger.info(s"Running $slaveSetup on slave: ${slaveNode.ip}")
        ScpAction(
          sourceN = None,
          srcPath = s"${GeneralConstants.setupScriptDir}/$slaveSetup",
          targetN = Some(slaveNode),
          targetPath = "~/setup.sh")

        //script will check if the specified hadoop version is valid.
        SshAction(slaveNode, s"source setup.sh $hadoopVersion")
        SshAction(slaveNode, "rm setup.sh")
        ""  //otherwise there is NPE
      }
    })

    Await.result(installMaster, Duration.Inf)
    installSlaves.map(sf => Await.result(sf, Duration.Inf))
  }
}
