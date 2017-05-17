package com.criteo.dev.cluster.config

import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpec, Matchers}

class TargetConfigParserSpec extends FlatSpec with Matchers {
  val config = ConfigFactory.load("target_test")
  "TargetConfigParser" should "parse target config" in {
    val res = TargetConfigParser(config)
    res.isSuccess shouldBe true
  }

  "TargetConfigConverter" should "convert TargetConfig to a Map[String, String]" in {
    val tConf = TargetConfigParser(config).value
    val res = TargetConfigConverter(tConf)
    println(res)
    val size = (tConf.aws.productArity + tConf.local.productArity + tConf.common.productArity)
    res.keys.size shouldEqual size
    res.values.toSet.size shouldEqual size
  }
}
