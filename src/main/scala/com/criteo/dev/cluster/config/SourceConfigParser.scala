package com.criteo.dev.cluster.config

import com.typesafe.config.Config
import configs.Result
import configs.Result.Success
import configs.syntax._

object SourceConfigParser {

  def apply(config: Config): Result[SourceConfig] = (
    config.get[String]("address") ~
    tables(config) ~
    copy(config) ~
    config.get[String]("s3.hdfs.scheme") ~
    config.get[Double]("default.sample.prob") ~
    config.get[Int]("default.partition.count") ~
    config.get[Option[Long]]("default.sample.size") ~
    config.get[String]("sample.database") ~
    config.get[Int]("parallelism") ~
    gateway(config)
  )(SourceConfig)

  def tables(config: Config): Result[List[TableConfig]] =
    config.get[List[Config]]("tables").flatMap(ts => Result.sequence(ts.map(table)))

  def table(config: Config): Result[TableConfig] = (
    config.get[String]("name") ~
    config.get[Option[Double]]("sample.prob") ~
    config.get[Option[Long]]("sample.size") ~
    config.get[List[List[String]]]("partitions").orElse(Success(List.empty)) ~
    config.get[Option[Int]]("partition.count")
  )(TableConfig)

  def copy(config: Config) = (
    config.get[String]("copy.scheme") ~
    config.get[Long]("copy.sample.threshold") ~
    config.get[List[String]]("copy.listeners").orElse(Success(List.empty)) ~
    config.get[List[String]]("sample.listeners").orElse(Success(List.empty))
  )(CopyConfig)

  def gateway(config: Config) = (
    config.get[List[String]]("gateway.docker.files") ~
    config.get[List[String]]("gateway.docker.ports") ~
    config.get[Config]("gateway").map(mapify(_) -- List("docker.files", "docker.ports"))
  )(GatewayConfig)
}
