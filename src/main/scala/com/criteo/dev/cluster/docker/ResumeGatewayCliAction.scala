package com.criteo.dev.cluster.docker

import com.criteo.dev.cluster.{CliAction, Public}
import org.slf4j.LoggerFactory

/**
  * Resumes a stopped gateway docker container.
  */
@Public object ResumeGatewayCliAction extends CliAction[Unit] {
  private val logger = LoggerFactory.getLogger(ResumeGatewayCliAction.getClass)

  override def command: String = "resume-gateway"

  override def usageArgs: List[Any] = List("container.id")

  override def help: String = "Resumes the docker gateway container, resuming from the state it was exited."

  override def applyInternal(args: List[String], conf: Map[String, String]): Unit = {
    val id = args(0)
    val command = s"docker start -ai $id"
    logger.info(command)
    //write command for script (dev-cluster) to run.
    DockerUtilities.writeDockerCommand(command)
  }
}
