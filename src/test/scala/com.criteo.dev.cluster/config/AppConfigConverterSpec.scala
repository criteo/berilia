package com.criteo.dev.cluster.config

import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpec, Matchers}

class AppConfigConverterSpec extends FlatSpec with Matchers {
  val config = ConfigFactory.load("app_test")
  "AppConfigConverter" should "convert the config to a Map" in {
    val conf = AppConfigParser(config).value
    val res = AppConfigConverter(conf)
    println(res)
    val size = (
      conf.productArity - 1 +
      conf.copyConfig.productArity - 1 +
      conf.gateway.productArity - 1 +
      conf.gateway.conf.size - 1 +
      conf.tables.filter(_.sampleProb.isDefined).size +
      conf.tables.filter(_.sampleSize.isDefined).size +
      conf.tables.filter(_.partitionCount.isDefined).size +
      conf.environment.productArity - 1 +
      conf.local.productArity - 1 +
      conf.aws.productArity - 1 +
      - 1 // skip defaultSampleSize
      - 1 // skip parallelism
      - 1 // skip overWriteIfExists
      - 1 // skip VolumeSpec
    )
    res.keys.size shouldEqual size
    res.values.size shouldEqual size
  }
}
