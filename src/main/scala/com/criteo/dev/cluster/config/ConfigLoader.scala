package com.criteo.dev.cluster.config

import java.net.URL

import com.criteo.dev.cluster.Public
import com.typesafe.config.{Config, ConfigFactory}
import configs.Result
import configs.syntax._

@Public
object ConfigLoader {
  def apply(
             app: URL,
             checkpoint: Option[URL]
           ): Result[GlobalConfig] = apply(
    ConfigFactory.parseURL(app).resolve(),
    checkpoint.map(ConfigFactory.parseURL(_).resolve)
  )

  def apply(
             app: Config,
             checkpoint: Option[Config]
           ): Result[GlobalConfig] = (
    AppConfigParser(app) ~
    OptionsParser(checkpoint)
  )(GlobalConfig(_,_)).map(_.withBackCompat)
}
