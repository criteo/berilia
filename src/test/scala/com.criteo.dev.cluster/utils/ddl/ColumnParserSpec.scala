package com.criteo.dev.cluster.utils.ddl

import org.scalatest.{FlatSpec, Matchers}

class ColumnParserSpec extends FlatSpec with Matchers with ColumnParser {
  it should "parse a column" in {
    val res = parse(column,
      """
        |col struct<age:int, name:string> COMMENT 'some comment'
      """.stripMargin)
    res.get shouldEqual Column("col", "struct<age:int, name:string>", Some("some comment"))
  }

  it should "parse columns" in {
    val res = parse(columns,
      """
        |(
        | name string,age int COMMENT 'comment,',
        | email string
        |)
      """.stripMargin)
    res.get shouldEqual List(
      Column("name", "string", None),
      Column("age", "int", Some("comment,")),
      Column("email", "string", None)
    )
  }

  it should "parse clustered by" in {
    val res = parse(clusteredBy,
      """
        |CLUSTERED BY(
        | a,
        | b
        |) SORTED BY(
        | a DESC
        |) INTO 32 BUCKETS
      """.stripMargin
    )
    res.get shouldEqual ClusteredBy(
      List("a", "b"),
      List(SortableColumn("a", DESC)),
      32
    )
  }

  it should "parse skewed by" in {
    val res = parse(skewedBy,
      """
        |SKEWED BY(
        | a,b
        |) ON ((1,'a'),(2,'b'))
        |STORED AS DIRECTORIES
      """.stripMargin
    )
    res.get shouldEqual SkewedBy(
      List("a", "b"),
      "(1,'a'),(2,'b')",
      asDirectories = true
    )
  }

}
