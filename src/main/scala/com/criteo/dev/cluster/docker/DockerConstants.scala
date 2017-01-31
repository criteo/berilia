package com.criteo.dev.cluster.docker

import com.criteo.dev.cluster.{GeneralConstants, GeneralUtilities}

/**
  * Constants for docker actions. (local cluster, or cluster gateway)
  */
object DockerConstants {

  //General constants of paths of docker files and other resources.
  def dockerBaseDir = "docker"

  //argument used in docker-files to define resources to copy to make the image.
  def hadoopVersion = "hadoop_version"

  def accessKeyArg = "accessKeyArg"  //See GeneralConstants for the corresponding arg in the hadoop-conf file

  def secretKeyArg = "secretKeyArg"  //See GeneralConstants for the corresponding arg in the hadoop-conf file

  def dataDirArg = "dataDirArg"  //See GeneralConstants for the corresponding arg in the hadoop-conf file

  def resource = "resource"

  def baseImage = "dev_cluster/base"

  def baseImageFinal = "dev_cluster/base:final"

  //used in final image, to keep metadata about config-specified ports to open beyond the default ones.
  def portLabel = "portLabel"

  //constants for creating gateway in docker.

  def gatewayImage = "dev_cluster/gateway"

  def gatewayImageFinal = "dev_cluster/gateway:final"

  def gatewayLabel = "application=dev_cluster_gateway"

  def gatewayDockerFiles = "gateway.docker.files"

  def gatewayDockerPorts = "gateway.docker.ports"

  def gatewayContribDir = "contrib-gateway"

  //unfortunately we cannot switch the terminal within the scala program, so dump the command
  //so the calling shell script can run it.
  //Make sure to change that script (./dev-cluster) should these change.
  def dockerTmpDir = "./temp-docker"
  def dockerRunShell = s"$dockerTmpDir/tmpDocker.sh"

  def dockerCommand = "create-gateway"

  def dockerResumeCommand = "resume-gateway"


  //constants for creating cluster in docker

  def localUbuntuClusterImage = "dev_cluster/local"

  def localUbuntuClusterImageFinal = "dev_cluster/local:final"

  def localClusterContainerLabel = "application=dev_cluster_local"

  def localClusterContribDir = "contrib-cluster"

  def localClusterDockerFiles = "target.local.docker.files"

  def localClusterPorts = "target.local.ports"

  //This is referenced in assembly bin.xml as it needs special permissions.
  //Make sure to change that file as well if this changes
  def dockerPubKey = "docker/keys/docker-key.pub"
  def dockerPrivateKey = s"docker/keys/docker-key"

  def clusterConfDir = "target.hadoop.conf"

  def localClusterPropPrefix = "target.local.cluster"

  def localTargetIp = "target.local.ip"

  def localDockerMachine = "target.local.docker.machine"

  //not exposed, but used internally to pass in the id for some actions.
  def localContainerId = "target.docker.container.id"

  def getAddressFull = localClusterPropPrefix + "." + GeneralConstants.address
  def getPortFull = localClusterPropPrefix + "." + GeneralConstants.port
  def getUserFull = localClusterPropPrefix + "." + GeneralConstants.user
  def getKeyFull = localClusterPropPrefix + "." + GeneralConstants.keyFile
}
