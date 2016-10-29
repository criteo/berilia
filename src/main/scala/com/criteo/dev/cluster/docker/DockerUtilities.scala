package com.criteo.dev.cluster.docker

import java.io.{File, PrintWriter}

import com.criteo.dev.cluster._
import org.slf4j.LoggerFactory

import scala.sys.process.Process

/**
  * Generic utilities for accessing Docker.
  */
object DockerUtilities {

  private val logger = LoggerFactory.getLogger(DockerUtilities.getClass)

  def getGatewayConf(conf: Map[String, String], key: String): String = {
    GeneralUtilities.getConfStrict(conf, key, GeneralConstants.gatewayProps)
  }


  def getLocalClusterConf(conf: Map[String, String], key: String): String = {
    GeneralUtilities.getConfStrict(conf, key, GeneralConstants.targetLocalProps)
  }

  def getGatewayHadoopConfDir(conf: Map[String, String], cluster: String): String = {
    val confDir = GeneralUtilities.getNonEmptyConf(conf, s"gateway.$cluster.conf")
    if (confDir.isDefined) {
      logger.info(s"Using hadoop configuration from gateway.$cluster.conf in " +
        GeneralConstants.gatewayProps)
      s"${GeneralConstants.hadoopConfSrcDir}/${confDir.get}"
    } else {
      logger.info(s"Using default hadoop configuration from ${GeneralConstants.targetCommonProps}")
      val confOverride = GeneralUtilities.getNonEmptyConf(conf, GeneralConstants.hadoopConfDirProp)
      if (confOverride.isDefined) {
        s"${GeneralConstants.hadoopConfSrcDir}/${confOverride.get}"
      } else {
        s"${GeneralConstants.hadoopConfSrcDir}/${GeneralConstants.clusterConfDefault}"
      }
    }
  }

  def isUnDefinedCluster(conf: Map[String, String], cluster: String): Boolean = {
    val confDir = conf.get(s"gateway.$cluster.conf")
    !confDir.isDefined
  }


  /**
    * Below are utilities mainly used for local-cluster dockers.
    */


  def imageExists(image: String) : Boolean = {
    val output = DevClusterProcess.process(s"docker images $image").!!
    output.contains(image)
  }


  def getSshHost(conf: Map[String, String]) : String = {
    val activeCmd = "docker-machine active"
    try {
      val active = DevClusterProcess.process(activeCmd).!!.stripLineEnd
      val ipCmd = s"docker-machine ip $active"
      DevClusterProcess.process(ipCmd).!!.stripLineEnd
    } catch {
      case e: Exception => {
        logger.info("Docker Toolbox compat: No docker-machine found, falling back to localhost (Assuming Docker on Mac)")
        "localhost"
      }
    }
  }

  def getSshPort(containerId: String) =  getPort(containerId, 22)

  def getPort(containerId : String, port: Int) : String = {
    val portCommand = s"docker port $containerId $port"
    val portMapping = DevClusterProcess.process(portCommand, skipLog = true).!!.stripLineEnd
    require(portMapping.split(":").length == 2,
      s"Error parsing docker command : $portCommand, which returned $portMapping")
    portMapping.split(":")(1)
  }

  def getSshCommand(conf: Map[String, String], host: String, containerId : String) : String = {
    val port = getSshPort(containerId)
    s"ssh -o StrictHostKeyChecking=no -i ${DockerConstants.dockerPrivateKey} root@$host -p $port"
  }

  def printClusterDockerContainerInfo(conf: Map[String, String], dockerMetas : Array[DockerMeta]) = {
    val host = getSshHost(conf)
    println
    dockerMetas.foreach(a => {
      println(s"Instance id: [${a.id}], State: [${a.dockerState}], Created: [${a.createDate}]")

      if (a.dockerState == DockerRunning) {
        println("%-20s%s".format("SSH command:", getSshCommand(conf, host, a.id)))
        println("Ports:")
        printClusterPorts(a.id, host, a.portMetas)
      }
      println
    })
  }

  def blockOnSsh(dockerNode : Node) = {
    logger.info("Waiting on Docker SSH Server..")
    while (!sshPortAvail(dockerNode)) {
      logger.info("SSH server not ready.")
      Thread.sleep(500)
    }
    logger.info("Docker SSH server is ready.")
  }

  def sshPortAvail(dockerNode: Node) : Boolean = {
    try {
      SshAction(dockerNode, "ls")
    } catch {
      case e: Exception => return false
    }
    true
  }

  def printGatewayDockerContainerInfo(conf: Map[String, String], dockerMetas : Array[DockerMeta]) = {
    val host = getSshHost(conf)
    println
    dockerMetas.foreach(a => {
      println(s"Instance id: [${a.id}], State: [${a.dockerState}], Created: [${a.createDate}]")

      if (a.dockerState == DockerRunning && a.portMetas.length != 0) {
        println("Ports:")
        printGatewayPorts(a.id, host, a.portMetas)
      }

      if (a.dockerState == DockerStopped) {
        println("Command to resume container:")
        println(s"./dev-cluster ${DockerConstants.dockerResumeCommand} ${a.id}")
      }
      println
    })
  }

  /**
    * @param stateString status returned from 'docker ps' command.
    * @return a modeled state
    */
  def getDockerState (stateString: String) : DockerState = {
    if (stateString.startsWith("Up")) {
      DockerRunning
    } else if (stateString.startsWith("Exited")) {
      DockerStopped
    } else {
      new OtherState(stateString)
    }
  }

  /**
    * These two dockerFiles build a base image,
    * that can be customized for specific images (ie, gateway, local-cluster)
    */
  def buildBaseDocker(conf: Map[String, String]) = {

    //install java
    DockerBuildAction(dockerFile = s"base/cluster-java",
      dockerImage = DockerConstants.baseImage)

    //install hadoop
    val hadoopVersion = GeneralUtilities.getConfStrict(conf, GeneralConstants.hadoopVersion, GeneralConstants.targetCommonProps)
    val dba = new DockerBuildAction(dockerFile = s"base/cluster-pseudo-hadoop",
      dockerImage = DockerConstants.baseImageFinal)
    dba.addArg(DockerConstants.hadoopVersion, hadoopVersion)
    dba.run
  }

  /**
    * Query docker to get a list of containers.
    *
    * @param label label to query for
    * @param id restrict to one container, if necessary.
    * @return
    */
  def getDockerContainerMetadata(label: String, id: Option[String] = None): Array[DockerMeta] = {

    //Use 'docker ps -a' command
    var psCommand = Seq("docker", "ps", "-a", "--filter", s"label=$label",
      "--format", "'{{.ID}}::{{.Status}}::{{.CreatedAt}}::{{.Label \"portLabel\"}}'")
    if (id.isDefined) {
      psCommand = psCommand ++ Seq("--filter", s"id=${id.get}")
    }

    //model the results
    val results = DevClusterProcess.processSeq(psCommand).!!
    if (results.equals("")) {
      Array()
    } else {
      results.split("\n").map(_.stripPrefix("'")).map(_.stripSuffix("'")).map(_.split("::", -1)).map(o => {
        if (o.length != 4) {
          throw new IllegalMonitorStateException(s"Failed to parse docker output: $o")
        }
        val id = o(0)
        val state = DockerUtilities.getDockerState(o(1))
        val date = o(2)
        val portMetas = DockerUtilities.getPortMetas(o(3))
        new DockerMeta(id, state, date, portMetas)
      })
    }
  }

  //----
  // Serialization and deserialization of port metadata.
  //----

  /**
    * Serializes port metadata into a string representation.
    */
  def getPortString(portMetas : Array[PortMeta]) : String = {
    portMetas.map(pm => s"${pm.description}/${pm.exposedPort}:${pm.port}").mkString(",")
  }

  /**
    * Deserializes a configured port meta into port metadata representation.
    * Can accept whitespaces.
    */
  def getPortMetas(conf: Map[String, String], key: String) : Array[PortMeta] = {
    val portString = conf.get(key)
    if (portString.isDefined) {
      getPortMetas(portString.get)
    } else {
      Array()
    }
  }

  /**
    * Deserializes a string representation of port metadata into modeled representation.
    */
  def getPortMetas(portString: String) : Array[PortMeta] ={
    if (portString.isEmpty) {
      Array()
    } else {
      val portDescriptions = portString.split(",").map(_.trim)
      portDescriptions.map(pd => getPortMeta(pd))
    }
  }

  /**
    * Deserializes string representation of one port metadata into modeled representation.
    *
    * @param portString
    * @return
    */
  def getPortMeta(portString : String) : PortMeta = {
    val split = portString.split("/").toList
    split match {
      case (description :: ports :: Nil) => {
        require(!description.toString.matches(".*\\s+.*"), s"Port description may not contain whitespace: $description")
        val exposedAndContainer = getExposedAndContainerPort(ports)
        PortMeta(description, exposedAndContainer._1, exposedAndContainer._2)
      }
      case (ports :: Nil) => {
        val exposedAndContainer = getExposedAndContainerPort(ports)
        PortMeta(exposedAndContainer._2, exposedAndContainer._1, exposedAndContainer._2)
      }
      case (_) => throw new IllegalArgumentException(s"Illegal port definition: $portString")
    }
  }

  /**
    * Deserializes string representation of an exposed port and container port into modeled representation (pair)
    */
  def getExposedAndContainerPort (portString: String) : (Option[String], String) = {
    val split = portString.split(":").toList
    split match {
      case (p1 :: p2 :: Nil) => (Some(p1), p2)
      case (p1 :: Nil) => (None, p1)
      case _ => throw new IllegalArgumentException(s"Illegal port definition: $portString")
    }
  }

  def printClusterPorts(id: String, ip: String, userPortMetas: Array[PortMeta]) = {
    val cdhPorts =  GeneralConstants.masterPorts.map(p => PortMeta(p._2, None, p._1))
    val allPorts = cdhPorts ++ userPortMetas
    allPorts.map(p => printPort(id, ip, p))
  }

  def printGatewayPorts(id: String, ip: String, userPortMetas: Array[PortMeta]) = {
    userPortMetas.map(p => printPort(id, ip, p))
  }

  def printPort(id: String, ip: String, portMeta: PortMeta) = {
    val psCommand = s"docker port $id ${portMeta.port}"
    val results = DevClusterProcess.process(psCommand.toString, skipLog=true).!!
    val address = results.stripLineEnd.replaceAll("0.0.0.0", ip)
    println("%-20s%s".format(s"${portMeta.description}:", address))
  }


  /**
    * Unfortunately we cannot change the terminal to be the docker container in scala program.
    * Hence we write this out to a shell script and then source it from the calling script (dev-cluster)
    **/
  def writeDockerCommand(command: String) = {
    GeneralUtilities.prepareDir(DockerConstants.dockerTmpDir)
    val dockerRunCommand = new File(DockerConstants.dockerRunShell)
    val writer = new PrintWriter(dockerRunCommand)
    writer.write(command)
    writer.close()
  }
}
