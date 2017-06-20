package com.criteo.dev.cluster

/**
  * General constants for the whole program
  * that are used in a wider manner.
  */
object GeneralConstants {

  def homeSysVar = "DEV_CLUSTER_HOME"

  def targetAwsProps = "./conf/target-aws.xml"

  def targetLocalProps = "./conf/target-local.xml"

  def gatewayProps = "./conf/gateway.xml"

  def sourceProps = "./conf/source.xml"

  def targetCommonProps = "./conf/target-common.xml"

  def targetTypeProp = "target.provider"

  //used in all the hadoop-conf files under ./hadoop-resources/hadoop-conf, and also docker files "copy-conf" and "copy-jar"
  def masterHostName = "dev-host"
  def master = "master"
  def local = "local"
  def accessKey = "accessKey"
  def secretKey = "secretKey"
  def dataDir = "dataDir"

  //actual physical directory for data volumes on the nodes
  def data = "data"

  //cluster type
  val localClusterType = "local-docker"
  val awsType = "aws"
  val s3Type = "s3"

  //The convention for nodes configuration properties.
  def keyFile = "key.file"
  def user = "user"
  def address = "address"
  def port = "port"
  def target = "target"

  //where to find aux-jars.  Used by both create-docker and create-complete-aws.
  def clusterConfDefault = "cluster-default"

  //Same in both source and target.  Used as relative path in source (./hadoop-resources), absolute path in target (/hadoop-resources)
  def auxJarSourceDir = "hadoop-resources/aux-jars"
  def auxJarTargetDir = "/hadoop-resources"

  def hadoopConfSrcDir = "hadoop-resources/hadoop-conf"
  def setupScriptDir = "hadoop-resources/setup-scripts"

  def auxJarProp = "target.hive.aux.jars"

  def hadoopVersion = "target.hadoop.version"

  def os = "target.base.os"

  def hadoopConfDirProp = "target.hadoop.conf.dir"

  //need to pattern match, so its a val.
  val ubuntu_trusty = "ubuntu-trusty"

  //ports of the pseudo-cluster
  def masterPorts = List(
    ("8042", "NodeManager"),
    ("8088", "ResourceManager"),
    ("19888", "HistoryServer"),
    ("50070", "NameNode"),
    ("50075", "DataNode"),
    ("18081", "SparkHistoryServer")
  )

  def slavePorts = List(
    ("8042", "NodeManager"),
    ("50075", "DataNode")
  )
}
