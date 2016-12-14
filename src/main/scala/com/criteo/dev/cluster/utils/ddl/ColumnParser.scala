package com.criteo.dev.cluster.utils.ddl

trait ColumnParser extends BaseParser {

  def column: Parser[Column] = ("`".? ~> validName <~ "`".?) ~ columnType ~ comment.? ^^ {
    case colName ~ colType ~ comment => Column(colName, colType, comment)
  }

  def columnType: Parser[String] = "\\w+<.*>".r | "\\w+".r

  def sortableColumn: Parser[SortableColumn] = validName ~ ("asc" | "desc").? ^^ {
    case colName ~ order => SortableColumn(colName, SortOrder(order.getOrElse("asc")))
  }

  def columns: Parser[List[Column]] = "(" ~> repsep(column, ",".r) <~ ")"

  def partitionedBy: Parser[List[Column]] = "partitioned by" ~> columns

  def clusteredBy: Parser[ClusteredBy] = "clustered by" ~>
    ("(" ~> repsep(validName, ",") <~ ")") ~
    ("sorted by" ~> "(" ~> repsep(sortableColumn, ",") <~ ")").? ~
    ("into" ~> int <~ "buckets") ^^ {
    case columns ~ sortedBy ~ numBuckets =>
      ClusteredBy(
        columns,
        sortedBy.getOrElse(List.empty),
        numBuckets
      )
  }

  def skewedBy: Parser[SkewedBy] = "skewed by" ~>
    ("(" ~> repsep(validName, ",") <~ ")") ~
    ("on" ~> "(" ~> """.*(?=\)(?!=,))""".r <~ ")") ~
    "stored as directories".? ^^ {
    case columns ~ on ~ dir =>
      SkewedBy(
        columns,
        on.mkString(""),
        dir.isDefined
      )
  }
}

case class Column(
                   name: String,
                   `type`: String, // TODO: better typed 'types'
                   comment: Option[String]
                 ) {
  def format = "`" + name + "`"
    s" ${`type`}${comment.map(" COMMENT '" + _ + "'").getOrElse("")}"
}

case class SortableColumn(
                           name: String,
                           order: SortOrder
                         ) {
  def format = s"$name ${order.toString}"
}

case class ClusteredBy(
                        columns: List[String],
                        sortedBy: List[SortableColumn],
                        numBuckets: Int
                      ) {
  def format =
    s"""CLUSTERED BY ${columns.mkString("(", ",", ")")}
        |  SORTED BY ${sortedBy.map(_.format).mkString("(", ",", ")")}
        |  INTO $numBuckets BUCKETS
    """.stripMargin
}

case class SkewedBy(
                     columns: List[String],
                     on: String, // TODO proper parsing of ON(...)
                     asDirectories: Boolean
                   ) {
  def format =
    s"""SKEWED BY ${columns.mkString("(", ",", ")")}
       | ON ($on)
       | ${if (asDirectories) "STORED AS DIRECTORIES" else ""}
     """.stripMargin
}

sealed trait SortOrder

object SortOrder {
  def apply(in: String) = in.toLowerCase match {
    case "desc" => DESC
    case "asc" => ASC
    case _ => InvalidOrder
  }
}

case object InvalidOrder extends SortOrder

case object DESC extends SortOrder

case object ASC extends SortOrder