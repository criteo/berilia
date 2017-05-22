package com.criteo.dev.cluster.utils.test

import java.io.File

import com.criteo.dev.cluster.config.ConfigLoader

trait LoadConfig {
  val config = ConfigLoader(
    new File("source.conf").toURI.toURL,
    new File("target.conf").toURI.toURL
  ).value

  val conf = config.backCompat
}
