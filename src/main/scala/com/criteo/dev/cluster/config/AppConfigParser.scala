package com.criteo.dev.cluster.config

import com.typesafe.config.Config
import configs.Result
import configs.Result.Success
import configs.syntax._

object AppConfigParser {
  def apply(config: Config): Result[AppConfig] =
    (
      config.get[String]("address") ~
      tables(config) ~
      copy(config) ~
      config.get[String]("s3.hdfs.scheme") ~
      config.get[Double]("default.sample.prob") ~
      config.get[Int]("default.partition.count") ~
      config.get[Option[Long]]("default.sample.size") ~
      config.get[String]("sample.database") ~
      parallelism(config) ~
      gateway(config) ~
      environment(config) ~
      local(config) ~
      aws(config)
    )(AppConfig)

  def tables(config: Config): Result[List[TableConfig]] =
    config.get[List[Config]]("tables").flatMap(ts => Result.sequence(ts.map(table)))

  def table(config: Config): Result[TableConfig] =
    (
      config.get[String]("name") ~
      config.get[Option[Double]]("sample.prob") ~
      config.get[Option[Long]]("sample.size") ~
      config.get[List[List[String]]]("partitions").orElse(Success(List.empty)) ~
      config.get[Option[Int]]("partition.count") ~
      config.get[Boolean]("skip.cleanup").orElse(Success(false))
    )(TableConfig)

  def copy(config: Config) =
    (
      config.get[String]("copy.scheme") ~
      config.get[Long]("copy.sample.threshold") ~
      config.get[Boolean]("copy.overwriteIfExists") ~
      config.get[List[String]]("copy.listeners").orElse(Success(List.empty)) ~
      config.get[List[String]]("sample.listeners").orElse(Success(List.empty))
    )(CopyConfig)

  def gateway(config: Config) =
    (
      config.get[List[String]]("gateway.docker.files") ~
      config.get[List[String]]("gateway.docker.ports") ~
      config.get[Config]("gateway").map(mapify(_) -- List("docker.files", "docker.ports"))
    )(GatewayConfig)

  def parallelism(config: Config) =
    (
      config.get[Int]("parallelism.table") ~
      config.get[Int]("parallelism.partition")
    )(ParallelismConfig)

  def environment(config: Config): Result[EnvironmentConfig] =
    (
      config.get[String]("hadoop.conf.dir") ~
      config.get[List[String]]("hive.aux.jars") ~
      config.get[String]("hadoop.version") ~
      config.get[String]("base.os")
    )(EnvironmentConfig)

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
