package com.criteo.dev.cluster.config

import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpec, Matchers}

class SourceConfigParserSpec extends FlatSpec with Matchers {
  val config = ConfigFactory.load("source_test")
  "SourceConfigParser" should "parse a config into a SourceConfig" in {
    val res = SourceConfigParser(config)
    res.isSuccess shouldBe true
    res.value.tables should have size (2)
    res.value.gateway.conf shouldEqual Map(
      "dc1.conf" -> "client_dc1"
    )
  }
  "SourceConfigConverter" should "convert to a Map[String, String]" in {
    val conf = SourceConfigParser(config).value
    val res = SourceConfigConverter(conf)
    println(res)
    val size = (
      conf.productArity - 1 +
      conf.copyConfig.productArity - 1 +
      conf.gateway.productArity - 1 +
      conf.gateway.conf.size - 1 +
      conf.tables.filter(_.sampleProb.isDefined).size +
      conf.tables.filter(_.sampleSize.isDefined).size
    )
    res.size shouldEqual size
    res.get("log.t2.sample.size") shouldBe Some("500")
  }
}
