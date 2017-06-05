package com.criteo.dev.cluster.utils.test

import java.io.File
import java.net.URL
import java.nio.file.Paths

import com.criteo.dev.cluster.config.ConfigLoader

trait LoadConfig {
  val config = ConfigLoader(
    ClassLoader.getSystemResource("source_test.conf").toURI.toURL,
    ClassLoader.getSystemResource("target_test.conf").toURI.toURL
  ).value

  val conf = config.backCompat
}
