package com.criteo.dev.cluster.docker

import com.criteo.dev.cluster.DevClusterProcess
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.sys.process.{Process, ProcessLogger}


/**
  * Builds docker image from docker file
  * @param dockerFile location of docker file (relative to /dockerFileDir)
  * @param dockerImage target image to build (should be specified in FROM in next dockerfile if used to layer images)
  */
class DockerBuildAction (dockerFile: String, dockerImage: String) {

  private val logger = LoggerFactory.getLogger(classOf[DockerBuildAction])

  private val processLogger = ProcessLogger(
    (e: String) => logger.info("err " + e))

  private val args = new ListBuffer[Pair[String, String]]
  private val ports = new ListBuffer[PortMeta]

  def addArg(key: String, value: String) = {
     args.+=(Pair(key, value))
  }

  def run() : Unit = {
    val sb = new StringBuilder("docker build")
    sb.append(s" -t $dockerImage")
    sb.append(s" -f ./${DockerConstants.dockerBaseDir}/$dockerFile")
    args.foreach(p =>  sb.append(s" --build-arg ${p._1}=${p._2}"))
    sb.append(s" ${DockerConstants.dockerBaseDir}")
    val out = DevClusterProcess.process(sb.toString).!
    if (out != 0) {
      throw new Exception("Failure running docker command.")
    }
  }
}

object DockerBuildAction {
  def apply(dockerFile: String, dockerImage: String) = {
    val dba = new DockerBuildAction(dockerFile, dockerImage)
    dba.run
  }
}
