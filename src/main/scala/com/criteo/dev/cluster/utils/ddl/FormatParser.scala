package com.criteo.dev.cluster.utils.ddl

trait FormatParser extends BaseParser {
  def rowFormat: Parser[RowFormat] = ("row format" ~> (serde | delimited)) | storedBy

  def storedBy: Parser[StoredBy] = "stored by" ~> hiveStringLiteral ~ ("with serdeproperties" ~> properties("=")).? ^^ {
    case name ~ props => StoredBy(name, props.getOrElse(Map.empty))
  }

  def delimiter: Parser[String] = "'[^']+'".r ^^ (_.replaceAll("'", ""))

  def delimited: Parser[Delimited] = "delimited" ~>
    ("fields terminated by" ~> delimiter).? ~
    ("escaped by" ~> delimiter).? ~
    ("collection items terminated by" ~> delimiter).? ~
    ("map keys terminated by" ~> delimiter).? ~
    ("lines terminated by" ~> delimiter).? ~
    ("null defined as" ~> delimiter).? ^^ {
    case fields ~ escaped ~ col ~ map ~ lines ~ nul =>
      Delimited(
        fields,
        escaped,
        col,
        map,
        lines,
        nul
      )
  }

  def serde: Parser[SerDe] =
    ("serde" ~> hiveStringLiteral) ~
      ("with serdeproperties" ~> properties("=")).? ^^ {
      case name ~ props => SerDe(name, props.getOrElse(Map.empty))
    }

  def storageFormat: Parser[StorageFormat] = "stored as" ~> (asFormat | ioFormat)

  def asFormat: Parser[StorageFormat] = ("textfile" | "sequencefile" | "orc" | "parquet" | "avro" | "rcfile") ^^ StorageFormat.apply

  def ioFormat: Parser[IOFormat] =
    ("inputformat" ~> hiveStringLiteral) ~ ("outputformat" ~> hiveStringLiteral) ^^ {
      case input ~ output => IOFormat(input, output)
    }
}

sealed trait RowFormat {
  def format: String
}

case class SerDe(name: String, properties: Map[String, String]) extends RowFormat {
  override def format =
    s"""${ParserConstants.rowFormatSerde} '$name'
        | ${properties.map { case (k, v) => s"'$k'='$v'" } toList match {
            case Nil => ""
            case props => s"WITH SERDEPROPERTIES ${props.mkString("(", ",", ")")}"
          }}
     """.stripMargin
}

case class Delimited(
                      fields: Option[String],
                      escaped: Option[String],
                      collection: Option[String],
                      mapKeys: Option[String],
                      lines: Option[String],
                      nullDefinedAs: Option[String]
                    ) extends RowFormat {
  override def format =
    s"""
       ROW FORMAT DELIMITED
       | ${prefix(fields, "FIELDS TERMINATED BY")}
       | ${prefix(escaped, "ESCAPED BY")}
       | ${prefix(collection, "COLLECTION ITEMS TERMINATED BY")}
       | ${prefix(mapKeys, "MAP KEYS TERMINATED BY")}
       | ${prefix(lines, "LINES TERMINATED BY")}
       | ${prefix(nullDefinedAs, "NULL DEFINED AS")}
     """.stripMargin

  def prefix(s : Option[String], prefix: String) = {
    s.map(prefix + " '" + _ + "'").getOrElse("")
  }

}

case class StoredBy(
                     name: String,
                     properties: Map[String, String]
                   ) extends RowFormat {
  override def format =
    s"""STORED BY '$name'
       |${properties.map { case (k, v) => s"'$k'='$v'" } toList match {
          case Nil => ""
          case props => s"WITH SERDEPROPERTIES ${props.mkString("(", ",", ")")}"
        }}
     """.stripMargin
}

sealed trait StorageFormat {
  def format = {
    val formatString = this match {
      case io: IOFormat => s"INPUTFORMAT '${io.input}' OUTPUTFORMAT '${io.output}'"
      case u: UNKNOWN => u.name
      case f => f.toString
    }
    s"STORED AS $formatString"
  }
}

object StorageFormat {
  def apply(in: String): StorageFormat = in.toLowerCase match {
    case "textfile" => TEXTFILE
    case "sequencefile" => SEQUENCEFILE
    case "orc" => ORC
    case "parquet" => PARQUET
    case "avro" => AVRO
    case "rcfile" => RCFILE
    case _ => UNKNOWN(in)
  }
}

case class UNKNOWN(name: String) extends StorageFormat

case object TEXTFILE extends StorageFormat

case object SEQUENCEFILE extends StorageFormat

case object ORC extends StorageFormat

case object PARQUET extends StorageFormat

case object AVRO extends StorageFormat

case object RCFILE extends StorageFormat

case class IOFormat(input: String, output: String) extends StorageFormat
