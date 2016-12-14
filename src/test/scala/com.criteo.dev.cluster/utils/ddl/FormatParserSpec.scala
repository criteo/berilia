package com.criteo.dev.cluster.utils.ddl

import org.scalatest.{FlatSpec, Matchers}

class FormatParserSpec extends FlatSpec with Matchers with FormatParser {
  it should "parse SerDe row format" in {
    val res = parse(format,
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
    val res = parse(format,
      """
        |ROW FORMAT DELIMITED
        |FIELDS TERMINATED BY '\t'
        |ESCAPED BY ';'
        |COLLECTION ITEMS TERMINATED BY '\n'
        |MAP KEYS TERMINATED BY ','
        |LINES TERMINATED BY 'c'
        |NULL DEFINED AS ' '
        |STORED AS TEXTFILE
      """.stripMargin
    )
    res.get shouldEqual Delimited(
      Some("\\t"),
      Some(";"),
      Some("\\n"),
      Some(","),
      Some("c"),
      Some(" "),
      TEXTFILE
    )
  }

  it should "parse stored by format" in {
    val res = parse(format,
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

