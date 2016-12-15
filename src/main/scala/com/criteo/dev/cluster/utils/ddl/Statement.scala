package com.criteo.dev.cluster.utils.ddl

sealed trait Statement

case class CreateTable(
                        isTemporary: Boolean,
                        isExternal: Boolean,
                        ifNotExists: Boolean,
                        database: Option[String],
                        table: String,
                        columns: List[Column],
                        comment: Option[String],
                        partitionedBy: List[Column],
                        clusteredBy: Option[ClusteredBy],
                        skewedBy: Option[SkewedBy],
                        rowFormat: Option[RowFormat],
                        storageFormat: Option[StorageFormat],
                        location: Option[String],
                        tblProperties: Map[String, String],
                        selectAs: Option[String]
                      ) extends Statement {
  def format: String = {
    val res = s"""CREATE
      |  ${if (isTemporary) "TEMPORARY" else ""}
      |  ${if (isExternal) "EXTERNAL" else ""}
      |  TABLE
      |  ${if (ifNotExists) "IF NOT EXISTS" else ""}
      |  ${database.map(_ + ".").getOrElse("")}$table
      |  ${columns.map(_.format).mkString("(", ",", ")")}
      |  ${comment.map("COMMENT '" + _ + "'").getOrElse("")}
      |  ${partitionedBy match {
            case Nil => ""
            case partitions => "PARTITIONED BY " + partitions.map(_.format).mkString("(", ",", ")")
          }}
      |  ${clusteredBy.map(_.format).getOrElse("")}
      |  ${skewedBy.map(_.format).getOrElse("")}
      |  ${rowFormat.map(_.format).getOrElse("")}
      |  ${storageFormat.map(_.format).getOrElse("")}
      |  ${location.map(s"LOCATION '" + _ + "'").getOrElse("")}
      |  ${tblProperties.map { case (k, v) => s"'$k'='$v'" } toList match {
             case Nil => ""
             case props => "TBLPROPERTIES " + props.mkString("(", ",", ")")
           }}
      |  ${selectAs.map("AS " + _).getOrElse("")}
        """.stripMargin
    res.split('\n').filter(s => !s.trim.isEmpty).mkString("\n")
  }
}
