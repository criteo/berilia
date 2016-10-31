package com.criteo.dev.cluster.docker

import com.criteo.dev.cluster.{CliAction, DevClusterProcess, Public}
import org.slf4j.LoggerFactory

import scala.sys.process.Process

/**
  * Destroy a local docker cluster
  */
@Public object DestroyLocalCliAction extends CliAction[Unit] {
  private val logger = LoggerFactory.getLogger(DestroyLocalCliAction.getClass)

  override def command: String = "destroy-local"

  override def usageArgs: List[Any] = List(Option("instanceId"))

  override def help: String = "Destroys a local cluster docker container. If instanceId specified, " +
    "only destroy that one container, else destroy all."

  override def applyInternal(args: List[String], conf: Map[String, String]): Unit = {

    //instance id is optional
    val instanceId = if (args.length == 1) Some(args(0)) else None

    val dockerMetas = DockerUtilities.getDockerContainerMetadata(
      DockerConstants.localClusterContainerLabel, instanceId)
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
