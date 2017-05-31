package com.criteo.dev.cluster.utils.test

import com.criteo.dev.cluster.config.ConfigLoader

trait LoadConfig {
  val config = ConfigLoader(
    ClassLoader.getSystemResource("source_test.conf").toURI.toURL,
    ClassLoader.getSystemResource("target_test.conf").toURI.toURL,
    None
  ).value

  val conf = config.backCompat
}
