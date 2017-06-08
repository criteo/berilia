package com.criteo.dev.cluster.source

import com.criteo.dev.cluster.Node
import com.criteo.dev.cluster.config.{GlobalConfig, TableConfig}
import com.criteo.dev.cluster.copy.GetMetadataAction

import scala.util.{Failure, Success, Try}

case class GetSourceSummaryAction(config: GlobalConfig, node: Node) {
  /**
    * Get the summary with HDFS file info of the source tables
    *
    * @return The table information related to HDFS
    */
  def apply(tables: List[TableConfig]): List[Either[InvalidTable, SourceTableInfo]] = {
    val conf = config.backCompat
    val getMetadata = new GetMetadataAction(conf, node)

    val (validTables, invalidTables) = tables
      .map { table =>
        (table.name, (table.name :: table.partitions.map(_.mkString("(", ",", ")")).mkString(" ") :: Nil).mkString(" "))
      }
      .map { case (tableName, spec) => (tableName, spec, Try(getMetadata(spec)))}
      .partition(_._3.isSuccess)
    val tableAndLocations = validTables
      .flatMap { case (_, _, Success(m)) =>
        if (m.partitions.size > 0)
          m.partitions.map(p => (m, p.location))
        else
          List((m, m.ddl.location.get))
      }
    tableAndLocations
      .zip(HDFSUtils.getFileSize(tableAndLocations.map(_._2), node))
      .groupBy { case ((m, _), _) => m }
      .foldLeft(List.empty[SourceTableInfo]) { case (acc, (table, results)) =>
        SourceTableInfo(
          table,
          TableHDFSInfo(
            table.database,
            table.ddl.table,
            results.map(_._2).sum,
            results.map(r => HDFSFileInfo(
              r._1._2,
              r._2
            )),
            table.partitions.size
          )
        ) :: acc
      }
      .map(Right(_)) ++
      invalidTables.map { case (tableName, spec, Failure(e)) =>
        Left(InvalidTable(tableName, spec, e.getMessage))
      }
  }
}
