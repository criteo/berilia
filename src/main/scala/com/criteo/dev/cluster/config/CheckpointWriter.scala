package com.criteo.dev.cluster.config

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions, ConfigValueFactory}

import collection.JavaConverters._

object CheckpointWriter {
  def apply(checkpoint: Checkpoint): Config = {
    import checkpoint._
    ConfigFactory.empty
      .withValue("created", ConfigValueFactory.fromAnyRef(created.toEpochMilli))
      .withValue("updated", ConfigValueFactory.fromAnyRef(updated.toEpochMilli))
      .withValue("todo", ConfigValueFactory.fromIterable(todo.asJava))
      .withValue("finished", ConfigValueFactory.fromIterable(finished.asJava))
      .withValue("failed", ConfigValueFactory.fromIterable(failed.asJava))
      .withValue("invalid", ConfigValueFactory.fromIterable(invalid.asJava))
  }

  def render(checkpoint: Checkpoint): String = {
    apply(checkpoint).root.render(ConfigRenderOptions.defaults().setJson(false).setOriginComments(false))
  }
}
