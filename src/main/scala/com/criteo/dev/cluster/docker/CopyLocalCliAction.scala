package com.criteo.dev.cluster.docker

import com.criteo.dev.cluster.{CliAction, GeneralUtilities, NodeFactory, Public}
import com.criteo.dev.cluster.copy._
import org.slf4j.LoggerFactory

/**
  * Copies data into docker container.
  */
@Public object CopyLocalCliAction extends CliAction[Unit] {

  def applyInternal(args: List[String], conf: Map[String, String]) : Unit = {
    val dockerContainerId = args(0)

    //check if docker container id matches a running docker instance.
    val dockerMeta = DockerUtilities.getDockerContainerMetadata(
      DockerConstants.localClusterContainerLabel,
      Some(dockerContainerId))
    val runningDockerMeta = dockerMeta.filter(_.dockerState == DockerRunning)
    require(runningDockerMeta.length == 1, s"Cannot find running docker container with id $dockerContainerId")

    //add some conf arguments expected by the DockerCopyFileAction command, as this is the argument used by 'docker cp'
    val newConf = collection.mutable.Map(conf.toArray:_*)
    newConf.+= (DockerConstants.localContainerId -> dockerContainerId)
    newConf += (DockerConstants.getAddressFull -> DockerUtilities.getSshHost(conf))
    newConf += (DockerConstants.getPortFull -> DockerUtilities.getSshPort(dockerContainerId))
    CopyAllAction(newConf.toMap)

    DockerUtilities.printClusterDockerContainerInfo(conf, dockerMeta)
  }

  override def command : String = "copy-local"

  override def usageArgs: List[Any] = List("container.id")

  override def help: String = "Copies sample data from gateway to node identified by container.id"

}
