package com.criteo.dev.cluster.config

case class GlobalConfig(
                         source: SourceConfig,
                         target: TargetConfig,
                         checkpoint: Option[Checkpoint],
                         backCompat: Map[String, String] = Map.empty
                       ) {
  def withBackCompat() = GlobalConfig(
    source,
    target,
    checkpoint,
    SourceConfigConverter(source) ++ TargetConfigConverter(target)
  )
}
