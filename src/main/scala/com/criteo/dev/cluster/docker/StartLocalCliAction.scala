package com.criteo.dev.cluster.docker

import com.criteo.dev.cluster.aws.AwsUtilities.NodeRole
import com.criteo.dev.cluster._
import com.criteo.dev.cluster.config.GlobalConfig
import org.slf4j.LoggerFactory

import scala.sys.process.Process

/**
  * Start docker container
  */
@Public object StartLocalCliAction extends CliAction[Unit] {

  private val logger = LoggerFactory.getLogger(StartLocalCliAction.getClass)

  override def command: String = "start-local"

  override def usageArgs: List[Any] = List(Option("instanceId"))

  override def help: String = "Starts a local cluster docker container. If instanceId specified, " +
    "only start that one container, else starts them all."

  override def applyInternal(args: List[String], config: GlobalConfig): Unit = {
    val conf = config.backCompat
    //instance id is optional
    val instanceId = if (args.length == 1) Some(args(0)) else None

    val dockerMetas = DockerUtilities.getDockerContainerMetadata(
      DockerConstants.localClusterContainerLabel,
      instanceId)
    dockerMetas.foreach(d => {
      val command = s"docker start ${d.id}"
      DevClusterProcess.process  (command).!!

      //add other required confs needed by the setup action (target ip, port)
      val dockerCluster = NodeFactory.getDockerNode(config.target.local, d)
      DockerUtilities.blockOnSsh(dockerCluster)
      StartServiceAction(dockerCluster, NodeRole.Master)

      //print out new docker container info.
      val dockerMetas = DockerUtilities.getDockerContainerMetadata(
        DockerConstants.localClusterContainerLabel,
        instanceId)
      DockerUtilities.printClusterDockerContainerInfo(conf, dockerMetas)
    })
  }
}
