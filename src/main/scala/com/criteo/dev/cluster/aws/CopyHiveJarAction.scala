package com.criteo.dev.cluster.aws

import com.criteo.dev.cluster._
import com.criteo.dev.cluster.aws.AwsUtilities.NodeRole
import com.criteo.dev.cluster.aws.AwsUtilities.NodeRole._
import com.criteo.dev.cluster.config.AWSConfig
import org.jclouds.compute.domain.NodeMetadata
import org.slf4j.LoggerFactory

/**
  * Copy over Hive AUX jars to the node(s) that needs it.
  *
  * For now, only master node has Hive installed.
  */
object CopyHiveJarAction {

  private val logger = LoggerFactory.getLogger(CopyHiveJarAction.getClass)

  /**
    *
    * @param config
    * @param conf for backward compat (remove in favor of config)
    * @param nodeMeta
    * @param nodeRole
    */
  def apply(config: AWSConfig, conf: Map[String, String], nodeMeta: NodeMetadata, nodeRole: NodeRole): Unit = {

    val target = NodeFactory.getAwsNode(config, nodeMeta)

    //copy extra hive jars, if set in 'aws.properties'
    val auxJars = GeneralUtilities.getNonEmptyConf(conf, GeneralConstants.auxJarProp)
    if (auxJars.isDefined && nodeRole == NodeRole.Master) {
      val jarList = GeneralUtilities.getAuxJarTargetList(conf, auxJars.get)

      //create the base directory on the target
      val createBaseDir = new SshMultiAction(target)
      createBaseDir.add(s"sudo mkdir -p ${GeneralConstants.auxJarTargetDir}/")
      createBaseDir.add(s"sudo chmod 777 ${GeneralConstants.auxJarTargetDir}/")
      createBaseDir.run()

      //copy over the configured hive jars to the base directory
      RsyncAction(
        srcPath = s"${GeneralConstants.auxJarSourceDir}/",
        targetN = target,
        targetPath = s"${GeneralConstants.auxJarTargetDir}")

      //set the jar path on hive-env.sh.  As the jar-directory was copied above, we need to use that in the path:  ./hadoop-resources/${jar-directory}
      val sshMultiAction = new SshMultiAction(target)
      sshMultiAction.add("sudo touch /etc/hive/conf/hive-env.sh")
      sshMultiAction.add(s"sudo echo 'export HIVE_AUX_JARS_PATH=$jarList' " +
        s"| sudo tee --append /etc/hive/conf/hive-env.sh")
      sshMultiAction.run()
    }
  }

}
