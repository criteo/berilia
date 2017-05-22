package com.criteo.dev.cluster.docker

import com.criteo.dev.cluster.config.GlobalConfig
import com.criteo.dev.cluster.{CliAction, Public}

/**
  * List gateways that can be restarted.
  */
@Public object ListGatewayCliAction extends CliAction[Unit] {
  override def command: String = "list-gateway"

  override def usageArgs: List[Any] = List()

  override def help: String = "Lists recently exited gateway docker containers, which can be resumed."

  override def applyInternal(args: List[String], config: GlobalConfig): Unit = {
    val conf = config.backCompat
    val dockerMetas = DockerUtilities.getDockerContainerMetadata(
      DockerConstants.gatewayLabel)
    DockerUtilities.printGatewayDockerContainerInfo(conf, dockerMetas)
  }
}
