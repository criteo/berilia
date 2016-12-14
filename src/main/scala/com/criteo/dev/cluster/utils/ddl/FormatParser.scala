package com.criteo.dev.cluster.utils.ddl

trait FormatParser extends BaseParser {
  def format: Parser[Format] = ("row format" ~> (serde | delimited)) | storedBy

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
    ("null defined as" ~> delimiter).? ~
    ("stored as" ~> storageFormat) ^^ {
    case fields ~ escaped ~ col ~ map ~ lines ~ nul ~ storageFormat =>
      Delimited(
        fields,
        escaped,
        col,
        map,
        lines,
        nul,
        storageFormat
      )
  }

  def serde: Parser[SerDe] =
    ("serde" ~> hiveStringLiteral) ~
      ("with serdeproperties" ~> properties("=")).? ~
      ("stored as" ~> storageFormat) ^^ {
      case name ~ props ~ format => SerDe(name, props.getOrElse(Map.empty), format)
    }

  def storageFormat: Parser[StorageFormat] = asFormat | ioFormat

  def asFormat: Parser[StorageFormat] = ("textfile" | "sequencefile" | "orc" | "parquet" | "avro" | "rcfile") ^^ StorageFormat.apply

  def ioFormat: Parser[IOFormat] =
    ("inputformat" ~> hiveStringLiteral) ~ ("outputformat" ~> hiveStringLiteral) ^^ {
      case input ~ output => IOFormat(input, output)
    }
}

sealed trait Format

case class StoredBy(
                     name: String,
                     properties: Map[String, String]
                   ) extends Format

case class SerDe(name: String, properties: Map[String, String], storageFormat: StorageFormat) extends Format

case class Delimited(
                      fields: Option[String],
                      escaped: Option[String],
                      collection: Option[String],
                      mapKeys: Option[String],
                      lines: Option[String],
                      nullDefinedAs: Option[String],
                      storageFormat: StorageFormat
                    ) extends Format


sealed trait StorageFormat

object StorageFormat {
  def apply(in: String): StorageFormat = in.toLowerCase match {
    case "textfile" => TEXTFILE
    case "sequencefile" => SEQUENCEFILE
    case "orc" => ORC
    case "parquet" => PARQUET
    case "avro" => AVRO
    case "rcfile" => RCFILE
    case _ => UNKNOWN
  }
}

case object UNKNOWN extends StorageFormat

case object TEXTFILE extends StorageFormat

case object SEQUENCEFILE extends StorageFormat

case object ORC extends StorageFormat

case object PARQUET extends StorageFormat

case object AVRO extends StorageFormat

case object RCFILE extends StorageFormat

case class IOFormat(input: String, output: String) extends StorageFormat
