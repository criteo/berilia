package com.criteo.dev.cluster.utils.ddl

import org.scalatest.{FlatSpec, Matchers}

class CreateTableParserSpec extends FlatSpec with Matchers with CreateTableParser {

  it should "parse location" in {
    val res = parse(location, "LOCATION '/root/folder'")
    res.get shouldEqual "/root/folder"
  }

  it should "parse tblproperties" in {
    val res = parse(tblProps,
      """
        |TBLPROPERTIES(
        | '1'='a',
        | '2'='b'
        |)
      """.stripMargin)
    res.get shouldEqual Map("1" -> "a", "2" -> "b")
  }

  it should "parse as" in {
    val res = parse(selectAs,
      """
        |AS
        |SELECT *
        |FROM a""".stripMargin)
    res.get shouldEqual
      """|SELECT *
         |FROM a""".stripMargin
  }

  it should "parse create table statement" in {
    val res = parse(createTable,
      """
        |CREATE TEMPORARY EXTERNAL TABLE IF NOT EXISTS db.t(
        | a int COMMENT 'comment for a',
        | b struct<name:string>
        |)
        |COMMENT 'comment for table'
        |PARTITIONED BY(
        | a int
        |)
        |CLUSTERED BY(
        | a
        |) SORTED BY (a DESC) INTO 32 BUCKETS
        |SKEWED BY(a)
        |ON (1,2,3)
        |STORED AS DIRECTORIES
        |ROW FORMAT SERDE 'org.serde'
        |STORED AS TEXTFILE
        |LOCATION
        |'/dir'
        |TBLPROPERTIES
        |(
        | '1'='a'
        |)
        |AS
        |SELECT * FROM a""".stripMargin
    )
    res.get shouldEqual CreateTable(
      isTemporary = true,
      isExternal = true,
      ifNotExists = true,
      database = Some("db"),
      table = "t",
      columns = List(Column("a", "int", Some("comment for a")), Column("b", "struct<name:string>", None)),
      comment = Some("comment for table"),
      partitionedBy = List(Column("a", "int", None)),
      clusteredBy = Some(ClusteredBy(List("a"), List(SortableColumn("a", DESC)), 32)),
      skewedBy = Some(SkewedBy(List("a"), "1,2,3", asDirectories = true)),
      format = Some(SerDe("org.serde", Map.empty, TEXTFILE)),
      location = Some("/dir"),
      tblProperties = Map("1" -> "a"),
      selectAs = Some("SELECT * FROM a")
    )
  }

  it should "format create statement" in {
    val createStmt = CreateTable(
      isTemporary = true,
      isExternal = true,
      ifNotExists = true,
      database = Some("db"),
      table = "t",
      columns = List(Column("a", "int", Some("comment for a")), Column("b", "struct<name:string>", None)),
      comment = Some("comment for table"),
      partitionedBy = List(Column("a", "int", None)),
      clusteredBy = Some(ClusteredBy(List("a"), List(SortableColumn("a", DESC)), 32)),
      skewedBy = Some(SkewedBy(List("a"), "1,2,3", asDirectories = true)),
      rowFormat = Some(SerDe("org.serde", Map.empty, TEXTFILE)),
      location = Some("/dir"),
      tblProperties = Map("1" -> "a"),
      selectAs = Some("SELECT * FROM a")
    )

    createStmt.format
  }
}
