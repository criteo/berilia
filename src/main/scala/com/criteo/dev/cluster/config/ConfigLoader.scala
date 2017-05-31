package com.criteo.dev.cluster.config

import java.net.URL

import com.criteo.dev.cluster.Public
import com.typesafe.config.{Config, ConfigFactory}
import configs.Result
import configs.Result.Success
import configs.syntax._

@Public
object ConfigLoader {
  def apply(
             source: URL,
             target: URL,
             checkpoint: Option[URL]
           ): Result[GlobalConfig] = apply(
    ConfigFactory.parseURL(source).resolve(),
    ConfigFactory.parseURL(target).resolve(),
    checkpoint.map(ConfigFactory.parseURL(_).resolve)
  )

  def apply(
             source: Config,
             target: Config,
             checkpoint: Option[Config]
           ): Result[GlobalConfig] = (
    SourceConfigParser(source) ~
    TargetConfigParser(target) ~
    checkpoint.fold(Success(None): Result[Option[Checkpoint]])(CheckpointParser.apply(_).map(Some(_)))
  )(GlobalConfig(_,_,_)).map(_.withBackCompat)
}
