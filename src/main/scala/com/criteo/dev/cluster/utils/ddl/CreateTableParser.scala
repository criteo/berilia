package com.criteo.dev.cluster.utils.ddl

trait CreateTableParser extends BaseParser with ColumnParser with RowFormatParser {
  def createTable: Parser[CreateTable] =
    "create" ~>
      "temporary".? ~
      "external".? ~
      ("table" ~> "if not exists".?) ~
      (validName <~ ".").? ~
      validName ~
      columns ~
      comment.? ~
      partitionedBy.? ~
      clusteredBy.? ~
      skewedBy.? ~
      rowFormat.? ~
      location.? ~
      tblProps.? ~
      selectAs.? ^^ {
      case temp ~ ext ~ ifNotExists ~ db ~ table ~ columns ~ comment ~ partitions ~ cluster ~ skew ~ rowFormat ~ location ~ tblProps ~ selectAs =>
        CreateTable(
          isTemporary = temp.isDefined,
          isExternal = ext.isDefined,
          ifNotExists = ifNotExists.isDefined,
          database = db,
          table = table,
          columns = columns,
          comment = comment,
          partitionedBy = partitions.getOrElse(List.empty),
          clusteredBy = cluster,
          skewedBy = skew,
          rowFormat = rowFormat,
          location = location,
          tblProperties = tblProps.getOrElse(Map.empty),
          selectAs = selectAs
        )
    }

  def location: Parser[String] = "location" ~> hiveStringLiteral

  def tblProps: Parser[Map[String, String]] = "tblproperties" ~> properties("=")

  def selectAs: Parser[String] = "AS" ~> "select" ~ rep(acceptIf(_ => true)(_ => "")) ^^ {
    case select ~ query => select + query.mkString("")
  }
}
