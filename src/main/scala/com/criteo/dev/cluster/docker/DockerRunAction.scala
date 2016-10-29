package com.criteo.dev.cluster.docker

import java.io.{File, PrintWriter}

import com.criteo.dev.cluster.{DevClusterProcess, GeneralConstants, GeneralUtilities}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.sys.process.{Process, ProcessLogger}

/**
  * Run a gateway container from the docker image created in previous steps.
  *
  * Can write in background mode, or foreground mode (which is actually leaving
  * a command for outside script to directly take the terminal into the new docker.)
  */
object DockerRunAction {

  private val logger = LoggerFactory.getLogger(classOf[DockerBuildAction])

  private val processLogger = ProcessLogger(
    (e: String) => logger.info("err " + e))

  private val ports = new ListBuffer[String]

  def apply(hosts: Map[String, String],
            image: String,
            mountDir: Option[String] = None,
            command: Option[String] = None,
            ports: Array[PortMeta],
            conf: Map[String, String],
            background: Boolean = false) : Option[String] = {
    val sb = new StringBuilder("docker run -P")
    if (background) {
      sb.append(" -d")
    } else {
      sb.append(" -it")
    }
    hosts.foreach {
      case (ip, name) => sb.append(s" --add-host=$name:$ip")
    }

    ports.foreach(p => {
      if (p.exposedPort.isDefined) {
        sb.append(s" -p ${p.exposedPort.get}:${p.port}")
      } else {
        sb.append(s" -p ${p.port}")
      }
    })

    if (mountDir.isDefined) {
      sb.append(s" -v ${mountDir.get}")
      sb.append(":/mount")
    }

    sb.append(s" $image")

    if (command.isDefined) {
      sb.append(s" ${command.get}")
    }

    val commandString = sb.toString
    println(commandString)

    if (background) {
      val output = DevClusterProcess.process(sb.toString).!!.stripLineEnd
      Some(output)
    } else {
      //write command to execute later (in dev-cluster script)
      DockerUtilities.writeDockerCommand(commandString)
      None
    }
  }
}
