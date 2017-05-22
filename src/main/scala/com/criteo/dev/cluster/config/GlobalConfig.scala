package com.criteo.dev.cluster.config

case class GlobalConfig(
                         source: SourceConfig,
                         target: TargetConfig,
                         backCompat: Map[String, String] = Map.empty
                       ) {
  def withBackCompat() = GlobalConfig(
    source,
    target,
    SourceConfigConverter(source) ++ TargetConfigConverter(target)
  )
}
