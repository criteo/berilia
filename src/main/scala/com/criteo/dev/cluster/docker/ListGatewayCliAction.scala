package com.criteo.dev.cluster.docker

import com.criteo.dev.cluster.{CliAction, Public}

/**
  * List gateways that can be restarted.
  */
@Public object ListGatewayCliAction extends CliAction[Unit] {
  override def command: String = "list-gateway"

  override def usageArgs: List[Any] = List()

  override def help: String = "Lists recently exited gateway docker containers, which can be resumed."

  override def applyInternal(args: List[String], conf: Map[String, String]): Unit = {
    val dockerMetas = DockerUtilities.getDockerContainerMetadata(
      DockerConstants.gatewayLabel)
    DockerUtilities.printGatewayDockerContainerInfo(conf, dockerMetas)
  }
}
