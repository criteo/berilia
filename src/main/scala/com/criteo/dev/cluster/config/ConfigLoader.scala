package com.criteo.dev.cluster.config

import java.net.URL

import com.criteo.dev.cluster.Public
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import configs.Result
import configs.syntax._

@Public
object ConfigLoader {
  def apply(source: URL, target: URL): Result[GlobalConfig] = apply(
    ConfigFactory.parseURL(source).resolve(),
    ConfigFactory.parseURL(target).resolve()
  )

  def apply(source: Config, target: Config): Result[GlobalConfig] = (
    SourceConfigParser(source) ~
    TargetConfigParser(target)
  )(GlobalConfig(_,_)).map(_.withBackCompat)


}
