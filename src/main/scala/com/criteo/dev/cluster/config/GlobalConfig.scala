package com.criteo.dev.cluster.config

case class GlobalConfig(
                         app: AppConfig,
                         options: Options,
                         backCompat: Map[String, String] = Map.empty
                       ) {
  def withBackCompat(): GlobalConfig = GlobalConfig(
    app,
    options,
    AppConfigConverter(app)
  )
}
