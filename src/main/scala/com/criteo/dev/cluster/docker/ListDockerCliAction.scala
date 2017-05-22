package com.criteo.dev.cluster.docker


import com.criteo.dev.cluster.config.GlobalConfig
import com.criteo.dev.cluster.{CliAction, Public}
import org.slf4j.LoggerFactory


/**
  * Lists all docker containers created by this cluster.
  */
@Public object ListDockerCliAction extends CliAction[Array[DockerMeta]] {

  private val logger = LoggerFactory.getLogger(ListDockerCliAction.getClass)

  override def command: String = "list-local"

  override def usageArgs: List[Any] = List()

  override def help: String = "Lists all running and stopped local dev-cluster docker containers."

  override def applyInternal(args: List[String], config: GlobalConfig): Array[DockerMeta] = {
    val conf = config.backCompat
    val dockerMetas =  DockerUtilities.getDockerContainerMetadata(
      DockerConstants.localClusterContainerLabel)
    DockerUtilities.printClusterDockerContainerInfo(conf, dockerMetas)
    dockerMetas
  }
}