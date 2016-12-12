package com.criteo.dev.cluster.utils.ddl

import org.scalatest.{FlatSpec, Matchers}

class BaseParserSpec extends FlatSpec with Matchers with BaseParser {

  it should "parse string literal without case sensitivity" in {
    parse("some", "SOME").successful shouldBe true
  }

  it should "parse hive string literal in DDL" in {
    val res = parse(hiveStringLiteral, "'hello.world'")
    res.get shouldEqual "hello.world"
  }

  it should "parse hive comment" in {
    val res = parse(comment, "COMMENT 'some comment'")
    res.get shouldEqual "some comment"
  }

  it should "parse int" in {
    val res = parse(int, "1024")
    res.get shouldEqual 1024
  }
}
