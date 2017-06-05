package com.criteo.dev.cluster

import com.criteo.dev.cluster.NodeType.NodeType
import com.criteo.dev.cluster.docker.{DockerConstants, DockerMeta, DockerUtilities}
import com.criteo.dev.cluster.aws.{AwsConstants, AwsNodeMeta, AwsUtilities}
import com.criteo.dev.cluster.config.{AWSConfig, LocalConfig, SourceConfig, TargetConfig}
import com.criteo.dev.cluster.copy.CopyConstants
import org.jclouds.compute.domain.NodeMetadata


case class Node(ip: String, user: Option[String], key: Option[String], port: Option[String], nodeType: NodeType)

object NodeType extends Enumeration {
  type NodeType = Value
  val S3, AWS, Local, User = Value
}


object NodeFactory {

  def getSourceFromConf(config: SourceConfig): Node = {
    val ip = config.address
    new Node(ip, None, None, None, NodeType.User)
  }
  //-- Begin Public API

  /**
    * TODO- Should not expose JClouds nodeMeta object in the API.
    */
  def getAwsNode(awsConfig: AWSConfig, nodeMeta: NodeMetadata) : Node = {
    val ip = AwsUtilities.ipAddress(nodeMeta)
    new Node(ip, Option(awsConfig.user), Option(awsConfig.keyFile), None, NodeType.AWS)
  }

  @Public def getAwsNode(awsConfig: AWSConfig, nodeMeta: AwsNodeMeta) : Node = {
    val ip = nodeMeta.publicIp
    new Node(ip, Option(awsConfig.user), Option(awsConfig.keyFile), None, NodeType.AWS)
  }

  @Public def getDockerNode(localConfig: LocalConfig, dockerMeta: DockerMeta) : Node = {
    val dockerIp = DockerUtilities.getSshHost
    val port = DockerUtilities.getSshPort(dockerMeta.id)
    val user = localConfig.clusterUser
    new Node(dockerIp, Option(user), Some(DockerConstants.dockerPrivateKey), Option(port), NodeType.Local)
  }

  @Public def getS3Node(bucketId: String) : Node = {
    new Node(bucketId, None, None, None, NodeType.S3)
  }
}