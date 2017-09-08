package com.criteo.dev.cluster.docker

import com.criteo.dev.cluster.aws.AwsUtilities.NodeRole
import com.criteo.dev.cluster.aws.{AwsConstants, AwsUtilities}
import com.criteo.dev.cluster.config.GlobalConfig
import com.criteo.dev.cluster.{CliAction, GeneralConstants, GeneralUtilities, Public}
import org.jclouds.compute.domain.NodeMetadata.Status
import org.slf4j.LoggerFactory

/**
  * Creates a gateway in docker.
  */
@Public object CreateGatewayCliAction extends CliAction[Unit] {

  private val logger = LoggerFactory.getLogger(CreateGatewayCliAction.getClass)
  override def command: String = "create-gateway"

  override def usageArgs: List[Any] = List("cluster", Option("mount.directory"))

  override def help: String = "Spawns a local docker container as a gateway to the given cluster.  This can be " +
    s"the instance.id of the cluster, or custom cluster defined in ${GeneralConstants.gatewayProps}.  " +
    "Also mounts the given mount.directory under /mount if provided."

  override def applyInternal(args: List[String], config: GlobalConfig): Unit = {
    val clusterId = args(0)
    val conf = config.backCompat

    val hosts: Map[String, String] =
      if (DockerUtilities.isUnDefinedCluster(conf, clusterId)) {
        val cluster = AwsUtilities.getUserCluster(conf, clusterId)
        val master = cluster.master
        require(master.getStatus().equals(Status.RUNNING), "No instances found in RUNNING state matching criteria.")

        //master host name is a constant
        val masterHosts = Map(AwsUtilities.ipAddress(master) -> GeneralConstants.masterHostName)
        cluster.slaves.foldLeft(masterHosts) ((map, slaveMeta) => {
          //slave hostnames are mapped via the tag
          map + (AwsUtilities.ipAddress(slaveMeta) -> slaveMeta.getUserMetadata.get(AwsConstants.hostName))
        })
      } else {
        Map()
      }

    //build base image
    DockerUtilities.buildBaseDocker(conf)

    //'bridge' from base image
    DockerBuildAction(
      dockerFile = "gateway/client-init",
      dockerImage = DockerConstants.gatewayImage
    )

    //copy configured gateway client hadoop conf, and hive aux jars
    val hadoopConfDir = DockerUtilities.getGatewayHadoopConfDir(conf, clusterId)
    val copyConfAction = new DockerCopyBuildAction(
      dockerFile = "gateway/client-copy-conf",
      dockerImage = DockerConstants.gatewayImage,
      resourcePath = hadoopConfDir)

    val accessId = GeneralUtilities.getNonEmptyConf(conf, AwsConstants.getFull(AwsConstants.accessId))
    val accessKey = GeneralUtilities.getNonEmptyConf(conf, AwsConstants.getFull(AwsConstants.accessKey))

    if (accessId.isDefined && accessKey.isDefined) {
      logger.info("AWS credentials defined, allowing direct HDFS access to S3 Buckets from this container.")
      copyConfAction.addArg(DockerConstants.accessKeyArg, accessId.get)
      copyConfAction.addArg(DockerConstants.secretKeyArg, accessKey.get)
    } else {
      logger.info("No AWS credentials defined, not allowing direct HDFS access to S3 Buckets from this container.")
    }
    copyConfAction.run

    //copy hive aux jars
    val auxJars = config.app.environment.hiveAuxJars
    if (!auxJars.isEmpty) {
      val jarList = GeneralUtilities.getAuxJarTargetList(auxJars.mkString(","))
      val copyJars = new DockerCopyBuildAction(
        dockerFile = "gateway/client-copy-jars",
        dockerImage= DockerConstants.gatewayImage,
        resourcePath = GeneralConstants.auxJarSourceDir)
      copyJars.addArg("jarList", jarList)
      copyJars.run()
    }

    val dockerWithUser = new DockerBuildAction(
      dockerFile = "gateway/client-with-user", dockerImage= DockerConstants.gatewayImage)
    dockerWithUser.addArg("orig_user", System.getenv("USER"))
    dockerWithUser.run()

    //Run each of the configured docker-file, each of which adds another layer.
    val dockerFiles = GeneralUtilities.getConfCSV(conf, DockerConstants.gatewayDockerFiles)
    dockerFiles.foreach(df => {
      DockerBuildAction(s"${DockerConstants.gatewayContribDir}/$df", DockerConstants.gatewayImage)
    })

    val mountDir = if (args.length == 2) Some(args(1)) else None  //mountDir is optional
    val ports = DockerUtilities.getPortMetas(conf, DockerConstants.gatewayDockerPorts)

    //final image
    val finalAction = new DockerBuildAction("gateway/client-final", DockerConstants.gatewayImageFinal)
    finalAction.addArg(DockerConstants.portLabel, DockerUtilities.getPortString(ports))
    finalAction.run

    //Run
    DockerRunAction(hosts,
        mountDir = mountDir,
        conf=conf,
        ports = ports,
        image=DockerConstants.gatewayImageFinal)
  }
}
