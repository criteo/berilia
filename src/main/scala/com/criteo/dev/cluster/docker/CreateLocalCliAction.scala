package com.criteo.dev.cluster.docker

import com.criteo.dev.cluster._
import com.criteo.dev.cluster.aws.AwsConstants
import com.criteo.dev.cluster.aws.AwsUtilities.NodeRole
import com.criteo.dev.cluster.config.{GlobalConfig, LocalConfig}
import org.slf4j.LoggerFactory


/**
  * Create a local docker cluster.
  */
@Public object CreateLocalCliAction extends CliAction[DockerMeta] {
  private val logger = LoggerFactory.getLogger(CreateLocalCliAction.getClass)

  override def command: String = "create-local"

  override def usageArgs: List[Any] = List(Option("mountDir"))

  override def help: String = "Creates a cluster using a local docker container and starts Hadoop services on it.  " +
    "If mount.dir is provided, directory will be mounted under /mount."

  override def applyInternal(args: List[String], config: GlobalConfig): DockerMeta = {
   // prereqs(conf)
    val conf = config.backCompat

    build(conf)
    start(args, config.target.local, conf)
  }

  def prereqs = {
    logger.info("Checking docker pre-reqs.")
    DevClusterProcess.process("docker ps").!!
    DockerUtilities.getSshHost
  }


  def build(conf: Map[String, String]) = {
    //build base image
    DockerUtilities.buildBaseDocker(conf)

    //'bridge' image
    DockerBuildAction(
      dockerFile = "local-cluster/cluster-init",
      dockerImage = DockerConstants.localUbuntuClusterImage)

    //add more tools needed for local-cluster
    DockerBuildAction(dockerFile = s"local-cluster/cluster-ssh",
      dockerImage = DockerConstants.localUbuntuClusterImage)

    DockerBuildAction(dockerFile = s"local-cluster/cluster-hive-metastore",
      dockerImage = DockerConstants.localUbuntuClusterImage)

    //copy hadoop configuration
    val confOverride = GeneralUtilities.getNonEmptyConf(conf, GeneralConstants.hadoopConfDirProp)
    val resourcePath =  if (confOverride.isDefined) {
      s"${GeneralConstants.hadoopConfSrcDir}/${confOverride.get}"
    } else {
      s"${GeneralConstants.hadoopConfSrcDir}/${GeneralConstants.clusterConfDefault}"
    }
    val copyConfAction = new DockerCopyBuildAction(
      dockerFile = "local-cluster/cluster-copy-conf",
      dockerImage = DockerConstants.localUbuntuClusterImage,
      resourcePath = resourcePath)

    val accessId = GeneralUtilities.getNonEmptyConf(conf, AwsConstants.getFull(AwsConstants.accessId))
    val accessKey = GeneralUtilities.getNonEmptyConf(conf, AwsConstants.getFull(AwsConstants.accessKey))
    if (accessId.isDefined && accessKey.isDefined) {
      logger.info("AWS credentials defined, allowing direct HDFS access to S3 Buckets from this container.")
      copyConfAction.addArg(DockerConstants.accessKeyArg, accessId.get)
      copyConfAction.addArg(DockerConstants.secretKeyArg, accessKey.get)
    } else {
      logger.info("No AWS credentials defined, not allowing direct HDFS access to S3 Buckets from this container.")
    }

    val dataDir = GeneralUtilities.getDataDir(List())
    copyConfAction.addArg(DockerConstants.dataDirArg, dataDir)
    copyConfAction.run


    //copy hive aux jars
    val auxJars = GeneralUtilities.getNonEmptyConf(conf, GeneralConstants.auxJarProp)
    if (auxJars.isDefined) {
      val jarList = GeneralUtilities.getAuxJarTargetList(auxJars.get)
      val copyJars = new DockerCopyBuildAction(
        dockerFile = "local-cluster/cluster-copy-jars",
        dockerImage= DockerConstants.localUbuntuClusterImage,
        resourcePath = GeneralConstants.auxJarSourceDir)
      copyJars.addArg("jarList", jarList)
      copyJars.run()
    }

    //copy public key (to allow passwordless ssh)
    DockerCopyBuildAction(
      dockerFile = "local-cluster/cluster-copy-pub",
      dockerImage = DockerConstants.localUbuntuClusterImage,
      resourcePath = DockerConstants.dockerPubKey)

    //Run each of the configured docker-file, each of which adds another layer.
    val dockerFiles = GeneralUtilities.getConfCSV(conf, DockerConstants.localClusterDockerFiles)
    dockerFiles.foreach(df => {
      DockerBuildAction(s"${DockerConstants.localClusterContribDir}/$df", DockerConstants.localUbuntuClusterImage)
    })

    //Final build
    val ports = DockerUtilities.getPortMetas(conf, DockerConstants.localClusterPorts)
    val finalAction = new DockerBuildAction(
      dockerFile = "local-cluster/cluster-final",
      dockerImage = DockerConstants.localUbuntuClusterImageFinal)
    finalAction.addArg(DockerConstants.portLabel, DockerUtilities.getPortString(ports))
    finalAction.run
  }


  def start(args: List[String], localConfig: LocalConfig, conf: Map[String, String]) : DockerMeta = {
    //get docker ip, mountDir, ports

    val mountDir = if (args.length == 1) Some(args(0)) else None
    val ports = DockerUtilities.getPortMetas(conf, DockerConstants.localClusterPorts)

    val containerId = DockerRunAction(
      hosts = Map("0.0.0.0" -> GeneralConstants.masterHostName),
      command = Some("/usr/sbin/sshd -D"),
      ports = ports,
      conf=conf,
      background = true,
      image=DockerConstants.localUbuntuClusterImageFinal,
      mountDir = mountDir)

    logger.info("Successfully started docker container")

    val dockerMetas = DockerUtilities.getDockerContainerMetadata(
      DockerConstants.localClusterContainerLabel, containerId)
    require (dockerMetas.size == 1, "Only one docker container may match container id")
    val dockerMeta = dockerMetas.last
    val dockerNode = NodeFactory.getDockerNode(localConfig, dockerMeta)

    DockerUtilities.blockOnSsh(dockerNode)
    StartServiceAction(dockerNode, NodeRole.Master)

    //print out new docker container info.
    DockerUtilities.printClusterDockerContainerInfo(conf, Array(dockerMeta))
    dockerMeta
  }
}
