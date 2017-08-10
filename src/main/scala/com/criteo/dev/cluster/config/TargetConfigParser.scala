package com.criteo.dev.cluster.config

import com.typesafe.config.Config
import configs.Result
import configs.syntax._

object TargetConfigParser {
  def apply(config: Config): Result[TargetConfig] =
    (
      common(config) ~
      local(config) ~
      aws(config)
    )(TargetConfig)

  def common(config: Config): Result[CommonConfig] =
    (
      config.get[String]("hadoop.conf.dir") ~
      config.get[List[String]]("hive.aux.jars") ~
      config.get[String]("hadoop.version") ~
      config.get[String]("base.os")
    )(CommonConfig)

  def local(config: Config): Result[LocalConfig] =
    (
      config.get[String]("local.cluster.user") ~
      config.get[List[String]]("local.ports") ~
      config.get[List[String]]("local.docker.files") ~
      config.get[String]("local.docker.containerId")
    )(LocalConfig)

  def aws(config: Config): Result[AWSConfig] =
    (
      config.get[String]("aws.access.id") ~
      config.get[String]("aws.access.key") ~
      config.get[String]("aws.instance.type") ~
      config.get[Config]("aws.volume.spec").flatMap(volumeSpec) ~
      config.get[Boolean]("aws.auto.volumes") ~
      config.get[String]("aws.user") ~
      config.get[String]("aws.region") ~
      config.get[String]("aws.subnet") ~
      config.get[String]("aws.security.group") ~
      config.get[String]("aws.key.pair") ~
      config.get[String]("aws.key.file") ~
      config.get[String]("aws.base.image.id") ~
      config.get[String]("s3.bucket.prefix")
    )(AWSConfig)

  def volumeSpec(config: Config): Result[VolumeSpec] =
    (
      config.get[List[Config]]("master").flatMap(c => Result.sequence(c.map(volume))) ~
      config.get[List[Config]]("slave").flatMap(c => Result.sequence(c.map(volume)))
    )(VolumeSpec)

  def volume(config: Config): Result[Volume] =
    (
      config.get[String]("name") ~
      config.get[Int]("size")
    )(Volume)
}