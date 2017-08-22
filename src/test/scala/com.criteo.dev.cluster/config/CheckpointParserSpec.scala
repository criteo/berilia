package com.criteo.dev.cluster.config

import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpec, Matchers}

class CheckpointParserSpec extends FlatSpec with Matchers {
  val config = ConfigFactory.load("checkpoint_test")
  "apply" should "parse a checkpoint" in {
    val res = CheckpointParser(config)
    res.isSuccess shouldBe true
    res.value.created.toEpochMilli shouldEqual 1000
    res.value.updated.toEpochMilli shouldEqual 2000
    res.value.finished shouldEqual Set("db.t1", "db.t2")
    res.value.todo shouldEqual Set("db.t3")
    res.value.failed shouldEqual Set("db.t0")
    res.value.invalid shouldEqual Set("db.t4")
  }
}
