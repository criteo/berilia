package com.criteo.dev.cluster.config

import com.typesafe.config.Config
import configs.Result
import configs.Result.Success

object OptionsParser {
  def apply(checkpoint: Option[Config]) = {
    checkpoint.fold(Success(None): Result[Option[Checkpoint]])(CheckpointParser.apply(_).map(Some(_))).map(
      Options(_)
    )
  }
}

