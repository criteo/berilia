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
                        format: Option[Format],
                        location: Option[String],
                        tblProperties: Map[String, String],
                        selectAs: Option[String]
                      ) extends Statement
