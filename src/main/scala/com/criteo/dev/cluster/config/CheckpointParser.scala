package com.criteo.dev.cluster.config

import java.time.Instant

import com.typesafe.config.Config
import configs.Result
import configs.syntax._

object CheckpointParser {
  def apply(config: Config): Result[Checkpoint] =
    (
      config.get[Long]("created").map(Instant.ofEpochMilli(_)) ~
      config.get[Long]("updated").map(Instant.ofEpochMilli(_)) ~
      config.get[Set[String]]("todo") ~
      config.get[Set[String]]("finished") ~
      config.get[Set[String]]("failed")
    )(Checkpoint)
}
