package com.criteo.dev.cluster.config

/**
  * Target config converter
  * For the backward compatibility of existing code
  */
object TargetConfigConverter {
  def apply(config: TargetConfig): Map[String, String] = Map(
    "target.hadoop.conf.dir" -> config.common.hadoopConfDir,
    "target.hive.aux.jars" -> config.common.hiveAuxJars.mkString(","),
    "target.hadoop.version" -> config.common.hadoopVersion,
    "target.base.os" -> config.common.baseOS,
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
  )
}
