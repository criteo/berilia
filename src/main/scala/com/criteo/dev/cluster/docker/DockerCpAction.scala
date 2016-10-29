package com.criteo.dev.cluster.docker

import com.criteo.dev.cluster.DevClusterProcess
import org.slf4j.LoggerFactory

import scala.sys.process.Process

/**
  * Supports one way copy from local to docker container for now.
  *
  * Can be enhanced to also support other way if need ever arises.
  */
object DockerCpAction {

  private val logger = LoggerFactory.getLogger(DockerCpAction.getClass)

  def apply(sourcePath: String, containerId: String, targetPath: String) = {
    val command = s"docker cp $sourcePath $containerId:$targetPath"
    DevClusterProcess.process(command).!!
  }
}
