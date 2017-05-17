package com.criteo.dev.cluster.config

import java.net.URL

import com.typesafe.config.{Config, ConfigFactory}
import configs.Result
import configs.syntax._

object ConfigLoader {
  def apply(source: URL, target: URL): Result[GlobalConfig] = apply(
    ConfigFactory.parseURL(source),
    ConfigFactory.parseURL(target)
  )

  def apply(source: Config, target: Config): Result[GlobalConfig] = (
    SourceConfigParser(source) ~
    TargetConfigParser(target)
  )(GlobalConfig(_,_)).map(_.withBackCompat)
}
