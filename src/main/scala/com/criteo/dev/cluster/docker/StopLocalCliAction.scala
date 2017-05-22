package com.criteo.dev.cluster.docker

import com.criteo.dev.cluster.config.GlobalConfig
import com.criteo.dev.cluster.{CliAction, DevClusterProcess, Public}
import org.slf4j.LoggerFactory

import scala.sys.process.Process

/**
  * Stops docker container
  */
@Public object StopLocalCliAction extends CliAction[Unit] {
  private val logger = LoggerFactory.getLogger(StopLocalCliAction.getClass)

  override def command: String = "stop-local"

  override def usageArgs: List[Any] = List(Option("instanceId"))

  override def help: String = "Stops a local cluster docker container. If instanceId specified, " +
    "only stop that one container, else stops them all."

  override def applyInternal(args: List[String], config: GlobalConfig): Unit = {

    //instance id is optional
    val instanceId = if (args.length == 2) Some(args(1)) else None

    val dockerMetas = DockerUtilities.getDockerContainerMetadata(
      DockerConstants.localClusterContainerLabel, instanceId)
    dockerMetas.foreach(d => {
      val command = s"docker stop ${d.id}"
      DevClusterProcess.process(command).!!
    })
  }
}
