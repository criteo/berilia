package com.criteo.dev.cluster.utils.ddl

import org.scalatest.{FlatSpec, Matchers}

class RowFormatParserSpec extends FlatSpec with Matchers with RowFormatParser {
  it should "parse SerDe row format" in {
    val res = parse(rowFormat,
      """
        |ROW FORMAT SERDE
        | 'org.apache'
        |WITH SERDEPROPERTIES(
        | 'a'='b'
        |)
        |STORED AS INPUTFORMAT
        | 'org.apache.input'
        |OUTPUTFORMAT
        | 'org.apache.output'
      """.stripMargin)
    res.get shouldEqual SerDe("org.apache", Map("a" -> "b"), IOFormat("org.apache.input", "org.apache.output"))
  }

  it should "parse Delimited row format" in {
    val res = parse(rowFormat,
      """
        |ROW FORMAT DELIMITED
        |FIELDS TERMINATED BY '\t'
        |ESCAPED BY ';'
        |COLLECTION ITEMS TERMINATED BY '\n'
        |MAP KEYS TERMINATED BY ','
        |LINES TERMINATED BY 'c'
        |NULL DEFINED AS ' '
      """.stripMargin
    )
    res.get shouldEqual Delimited(
      Some("\\t"),
      Some(";"),
      Some("\\n"),
      Some(","),
      Some("c"),
      Some(" ")
    )
  }

  it should "parse stored by format" in {
    val res = parse(rowFormat,
      """
        |STORED BY 'org.storage'
        |WITH SERDEPROPERTIES(
        |"a" = "b",
        |"c" = "d"
        |)
      """.stripMargin
    )
    res.get shouldEqual StoredBy(
      "org.storage",
      Map("a" -> "b", "c" -> "d")
    )
  }

}

