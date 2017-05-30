package com.criteo.dev.cluster.docker

import com.criteo.dev.cluster.config.GlobalConfig
import com.criteo.dev.cluster.copy._
import com.criteo.dev.cluster.{CliAction, NodeFactory, Public}

/**
  * Copies data into docker container.
  */
@Public object CopyLocalCliAction extends CliAction[Unit] {

  def applyInternal(args: List[String], config: GlobalConfig) : Unit = {
    val dockerContainerId = args(0)
    val conf = config.backCompat
    //check if docker container id matches a running docker instance.
    val dockerMeta = DockerUtilities.getDockerContainerMetadata(
      DockerConstants.localClusterContainerLabel,
      Some(dockerContainerId))
    val runningDockerMeta = dockerMeta.filter(_.dockerState == DockerRunning)
    require(runningDockerMeta.length == 1, s"Cannot find running docker container with id $dockerContainerId")

    //add some conf arguments expected by the DockerCopyFileAction command, as this is the argument used by 'docker cp'
    val target = NodeFactory.getDockerNode(conf, runningDockerMeta(0))
    val source = NodeFactory.getSourceFromConf(conf)

    //TODO- hack, the docker id should go into "target" Node object, but currently Node does not have sub-classes
    CopyAllAction(config, conf + (DockerConstants.localContainerId -> runningDockerMeta(0).id), source, target)

    DockerUtilities.printClusterDockerContainerInfo(conf, dockerMeta)
  }

  override def command : String = "copy-local"

  override def usageArgs: List[Any] = List("container.id")

  override def help: String = "Copies sample data from gateway to node identified by container.id"

}
