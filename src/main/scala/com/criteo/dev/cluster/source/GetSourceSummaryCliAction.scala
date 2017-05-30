package com.criteo.dev.cluster.source

import com.criteo.dev.cluster.config.GlobalConfig
import com.criteo.dev.cluster.{CliAction, NodeFactory}
import org.slf4j.LoggerFactory

object GetSourceSummaryCliAction extends CliAction[List[Either[InvalidTable, SourceTableInfo]]] {
  private val logger = LoggerFactory.getLogger(this.getClass)

  override def command: String = "get-source-summary"

  override def usageArgs: List[Any] = List.empty

  override def help: String = "Get summary of source tables"

  override def applyInternal(args: List[String], config: GlobalConfig): List[Either[InvalidTable, SourceTableInfo]] = {
    val conf = config.backCompat
    logger.info("Getting the summary of source tables")
    val source = NodeFactory.getSourceFromConf(conf)
    val getSourceSummary = GetSourceSummaryAction(config, source)
    val summary = getSourceSummary(config.source.tables)
    printSummary(summary)
    summary
  }

  def printSummary(summary: List[Either[InvalidTable, SourceTableInfo]]): Unit = {
    println("Source tables summary")
    val (invalid, valid) = summary.partition(_.isLeft)
    invalid.map(_.left.get).foreach { case InvalidTable(input, message) =>
      println(s"$input is invalid, reason: $message")
    }
    valid.map(_.right.get) foreach { case SourceTableInfo(_, TableHDFSInfo(db, table, size, files, partitions)) =>
      println(s"$db.$table is available, size: $size Bytes, files: ${files.size}, partitions: $partitions")
    }
    val totalSize = valid.map(_.right.get.hdfsInfo.size).sum
    println(s"Invalid tables: ${invalid.size}, valid tables: ${valid.size}")
    println(s"Total size: $totalSize Bytes")
  }
}
