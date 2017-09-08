package com.criteo.dev.cluster.config

import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpec, Matchers}

class AppConfigParserSpec extends FlatSpec with Matchers {
  val config = ConfigFactory.load("app_test")
  "AppConfigParser" should "parse a config into an AppConfig instance" in {
    val res = AppConfigParser(config)
    res.isSuccess shouldBe true
  }
}
