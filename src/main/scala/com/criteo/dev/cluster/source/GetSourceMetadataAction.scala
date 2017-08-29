package com.criteo.dev.cluster.source

import com.criteo.dev.cluster.Node
import com.criteo.dev.cluster.config.{GlobalConfig, TableConfig}
import com.criteo.dev.cluster.copy.GetMetadataAction

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.util.{Failure, Success, Try}

/**
  * Get the summary with HDFS file info of the source tables
  */
case class GetSourceMetadataAction(config: GlobalConfig, node: Node) {
  /**
    *
    * @param tables         List of tables
    * @param useLocalScheme Whether get metadata from local environment
    * @return
    */
  def apply(tables: List[TableConfig], useLocalScheme: Boolean = config.source.isLocalScheme): List[Either[InvalidTable, FullTableInfo]] = {
    val conf = config.backCompat
    val getMetadata = new GetMetadataAction(config, conf, node)

    // configure parallel execution
    val parTables = tables.par
    parTables.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(config.source.parallelism.table))
    val (validTables, invalidTables) = parTables
      .map { table =>
        val (tableName, spec) = (table.name, (table.name :: table.partitions.map(_.mkString("(", ",", ")")).mkString(" ") :: Nil).mkString(" "))
        (tableName, spec, Try(getMetadata(spec, useLocalScheme)))
      }
      .toList
      .partition(_._3.isSuccess)
    val tableAndLocations = validTables
      .flatMap { case (_, _, Success(m)) =>
        if (m.partitions.size > 0)
          m.partitions.map(p => (m, p.location))
        else
          List((m, m.ddl.location.get))
      }
    tableAndLocations
      .zip(
        if (useLocalScheme)
          HDFSUtils.getFileSize(tableAndLocations.map(_._2))
        else
          HDFSUtils.getFileSize(tableAndLocations.map(_._2), node)
      )
      .groupBy { case ((m, _), _) => m }
      .foldLeft(List.empty[FullTableInfo]) { case (acc, (table, results)) =>
        FullTableInfo(
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
