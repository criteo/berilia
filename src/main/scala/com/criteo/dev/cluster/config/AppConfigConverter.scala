package com.criteo.dev.cluster.config

/**
  * For the backward compatibility
  */
object AppConfigConverter {
  def apply(config: AppConfig): Map[String, String] = Map(
    "source.address" -> config.address,
    "source.tables" -> config.tables.map(t => t.name + t.partitions.map(_.mkString(" (", ",", ")")).mkString).mkString(";"),
    "default.partition.count" -> config.defaultPartitionCount.toString,
    "default.sample.prob" -> config.defaultSampleProb.toString,
    "source.sample.database" -> config.sampleDatabase,
    "source.copy.listeners" -> config.copyConfig.listeners.mkString(","),
    "source.sample.listeners" -> config.copyConfig.sampleListeners.mkString(","),
    "source.copy.sample.threshold" -> config.copyConfig.sampleThreshold.toString,
    "source.s3.hdfs.scheme" -> config.s3HDFSScheme,
    "source.copy.scheme" -> config.copyConfig.scheme,
    "gateway.docker.files" -> config.gateway.dockerFiles.mkString(","),
    "gateway.docker.ports" -> config.gateway.dockerPorts.mkString(","),
    "target.hadoop.conf.dir" -> config.environment.hadoopConfDir,
    "target.hive.aux.jars" -> config.environment.hiveAuxJars.mkString(","),
    "target.hadoop.version" -> config.environment.hadoopVersion,
    "target.base.os" -> config.environment.baseOS,
    "target.local.cluster.user" -> config.local.clusterUser,
    "target.local.ports" -> config.local.ports.mkString(","),
    "target.local.docker.files" -> config.local.dockerFiles.mkString(","),
    "target.local.docker.containerId" -> config.local.dockerContainerId,
    "target.aws.access.id" -> config.aws.accessId,
    "target.aws.access.key" -> config.aws.accessKey,
    "target.aws.instance.type" -> config.aws.instanceType,
    "target.aws.auto.volumes" -> config.aws.autoVolumes.toString,
    "target.aws.user" -> config.aws.user,
    "target.aws.region" -> config.aws.region,
    "target.aws.subnet" -> config.aws.subnet,
    "target.aws.security.group" -> config.aws.securityGroup,
    "target.aws.key.pair" -> config.aws.keyPair,
    "target.aws.key.file" -> config.aws.keyFile,
    "target.aws.base.image.id" -> config.aws.baseImageId,
    "s3.bucket.prefix" -> config.aws.s3BucketPrefix
  ) ++ {
    // table.sample.prob -> number
    // table.sample.size -> number
    config.tables.collect {
      case TableConfig(name, _, Some(sampleSize), _, _, _) => (s"$name.sample.size", sampleSize.toString)
      case TableConfig(name, Some(sampleProb), _, _, _, _) => (s"$name.sample.prob", sampleProb.toString)
    }
  } ++ {
    // table.partition.count -> number
    config.tables.collect {
      case TableConfig(name, _, _, _, Some(partitionCount), _) => (s"$name.partition.count", partitionCount.toString)
    }
  } ++ config.gateway.conf.map { case (k, v) => (s"gateway.$k", v) }
}
