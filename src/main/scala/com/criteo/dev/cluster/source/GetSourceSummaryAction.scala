package com.criteo.dev.cluster.source

import com.criteo.dev.cluster.Node
import com.criteo.dev.cluster.copy.GetMetadataAction

import scala.util.{Failure, Success, Try}

case class GetSourceSummaryAction(conf: Map[String, String], node: Node) {
  /**
    * Get the summary with HDFS file info of the source tables
    *
    * @return The table information related to HDFS
    */
  def apply(): List[Either[InvalidTable, TableHDFSInfo]] = {
    val getMetadata = new GetMetadataAction(conf, node)
    val getFileSizes = GetFileSizeAction(conf, node)

    val (validTables, invalidTables) = conf("source.tables")
      .split(";")
      .map(_.trim)
      .map(tableSpec => (tableSpec, Try(getMetadata(tableSpec))))
      .toList
      .partition(_._2.isSuccess)
    val tableAndLocations = validTables
      .flatMap { case (_, Success(m)) =>
        if (m.partitions.size > 0)
          m.partitions.map(p => (m, p.location))
        else
          List((m, m.ddl.location.get))
      }
    tableAndLocations
      .zip(getFileSizes(tableAndLocations.map(_._2)))
      .groupBy { case ((m, _), _) => m }
      .foldLeft(List.empty[TableHDFSInfo]) { case (acc, (table, results)) =>
        TableHDFSInfo(
          table.database,
          table.ddl.table,
          results.map(_._2).sum,
          results.map(r => HDFSFileInfo(
            r._1._2,
            r._2
          )),
          table.partitions.size
        ) :: acc
      }
      .map(Right(_)) ++
      invalidTables.map { case (tableSpec, Failure(e)) =>
        Left(InvalidTable(tableSpec, e.getMessage))
      }
  }
}
