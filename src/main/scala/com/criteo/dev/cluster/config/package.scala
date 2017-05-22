package com.criteo.dev.cluster

import com.typesafe.config.Config

import scala.collection.JavaConverters._

package object config {
  /**
    * Turn a config to a map of string-string pairs
    * @param config The config
    * @return A map of key-values
    */
  def mapify(config: Config): Map[String, String] =
    config
      .entrySet()
      .asScala
      .map(entry => (entry.getKey, entry.getValue.unwrapped().toString))
      .toMap
}
