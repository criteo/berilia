package com.criteo.dev.cluster.docker

import com.criteo.dev.cluster.config.GlobalConfig
import com.criteo.dev.cluster.{CliAction, DevClusterProcess, Public}
import org.slf4j.LoggerFactory

import scala.sys.process.Process

/**
  * Destroys docker gateway container.
  */
@Public object DestroyGatewayCliAction extends CliAction[Unit] {
  private val logger = LoggerFactory.getLogger(DestroyGatewayCliAction.getClass)

  override def command: String = "destroy-gateway"

  override def usageArgs: List[Any] = List(Option("container.id"))

  override def help: String = "Destroys local gateway docker containers. If container.id specified, " +
    "only destroy that one container, else destroy all."

  override def applyInternal(args: List[String], config: GlobalConfig): Unit = {

    //instance id is optional
    val instanceId = if (args.length == 1) Some(args(0)) else None

    val dockerMetas = DockerUtilities.getDockerContainerMetadata(
      DockerConstants.gatewayLabel, instanceId)
    dockerMetas.foreach(d => {
      logger.info(s"Destroying ${d.id}")
      val stopCommand = s"docker stop ${d.id}"
      val rmCommand = s"docker rm ${d.id}"

      //stop the docker container
      DevClusterProcess.process(stopCommand).!!

      //remove the docker container
      DevClusterProcess.process(rmCommand).!!
    })
  }
}
