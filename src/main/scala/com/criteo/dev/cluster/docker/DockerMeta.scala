package com.criteo.dev.cluster.docker

import com.criteo.dev.cluster.Public

/**
  * Represents docker instances.
  */
@Public case class DockerMeta(id: String, dockerState: DockerState, createDate: String, portMetas: Array[PortMeta])

@Public sealed abstract class DockerState(name: String) {
  override def toString = name
}
@Public object DockerRunning extends DockerState("Running")
@Public object DockerStopped extends DockerState("Stopped")

@Public class OtherState(name: String) extends DockerState(name)



@Public case class PortMeta(description:String, exposedPort: Option[String], port: String)