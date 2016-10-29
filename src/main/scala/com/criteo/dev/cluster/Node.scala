package com.criteo.dev.cluster

import com.criteo.dev.cluster.docker.{DockerConstants, DockerMeta, DockerUtilities}
import com.criteo.dev.cluster.aws.{AwsConstants, AwsNodeMeta, AwsUtilities}
import com.criteo.dev.cluster.copy.CopyConstants
import org.jclouds.compute.domain.NodeMetadata


case class Node(ip: String, user: Option[String], key: Option[String], port: Option[String])

object NodeFactory {

  //source node configuration always provided by user in gateway.xml
  def getSource(conf: Map[String, String]): Node = {
    val ip = GeneralUtilities.getConfStrict(conf, CopyConstants.getAddressFull,
      GeneralConstants.sourceProps)
    val user = conf.get(CopyConstants.getUserFull)
    val keyFile = conf.get(CopyConstants.getKeyFileFull)
    val port = conf.get(CopyConstants.getPortFull)
    new Node(ip, user, keyFile, port)
  }


  //Target configs are injected by the code based on target type.
  def getTarget(conf: Map[String, String]): Node = {
    val target = GeneralUtilities.getConfStrict(conf,
      GeneralConstants.targetTypeProp, "Internally provided.")
    target match {
      case (GeneralConstants.awsType) => getAwsNode(conf)
      case (GeneralConstants.localClusterType) => getLocalClusterNode(conf)
      case (GeneralConstants.s3Type) => getS3Node(conf)
      case _ => throw new IllegalArgumentException(s"Wrong ${GeneralConstants.targetTypeProp} : $target")
    }
  }

  private def getAwsNode(conf: Map[String, String]) = {
    val ip = GeneralUtilities.getConfStrict(conf, AwsConstants.getAddressFull,
      GeneralConstants.targetAwsProps)
    val user = conf.get(AwsConstants.getUserFull)
    val key = conf.get(AwsConstants.getKeyFileFull)
    new Node(ip, user, key, None)
  }


  /**
    * Should not expose JClouds nodeMeta object in the API.  Migrate to AWSNodeMeta
    */
  @Deprecated
  def getAwsNode(conf: Map[String, String], nodeMeta: NodeMetadata) : Node = {
    val ip = AwsUtilities.ipAddress(nodeMeta)
    val newConf = AwsUtilities.getConfWithIp(conf, ip)
    NodeFactory.getAwsNode(newConf)
  }

  @Public def getAwsNode(conf: Map[String, String], nodeMeta: AwsNodeMeta) : Node = {
    val ip = nodeMeta.publicIp
    val newConf = AwsUtilities.getConfWithIp(conf, ip)
    NodeFactory.getAwsNode(newConf)
  }

  private def getLocalClusterNode(conf: Map[String, String]) = {
    val ip = GeneralUtilities.getConfStrict(conf, DockerConstants.getAddressFull,
      GeneralConstants.targetLocalProps)
    val user = conf.get(DockerConstants.getUserFull)
    val port = conf.get(DockerConstants.getPortFull)
    new Node(ip, user, Some(DockerConstants.dockerPrivateKey), port)
  }

  @Public def getDockerNode(conf: Map[String, String], dockerMeta: DockerMeta) : Node = {
    val dockerIp = DockerUtilities.getSshHost(conf)
    val newConf = collection.mutable.Map[String, String](conf.toArray:_*)
    newConf += (DockerConstants.getAddressFull -> dockerIp)
    newConf += (DockerConstants.getPortFull -> DockerUtilities.getSshPort(dockerMeta.id))
    val newConfMap = newConf.toMap
    NodeFactory.getLocalClusterNode(newConfMap)
  }

  @Public def getS3Node(conf: Map[String, String]) : Node = {
    val ip = conf.get(s"${AwsConstants.bucketKeyPrefix}.${GeneralConstants.address}")
    require (ip.isDefined, "Internal error, address should have been passed in")
    new Node(ip.get, None, None, None)
  }
}