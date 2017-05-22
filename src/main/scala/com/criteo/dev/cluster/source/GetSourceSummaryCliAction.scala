package com.criteo.dev.cluster.source

import com.criteo.dev.cluster.config.GlobalConfig
import com.criteo.dev.cluster.{CliAction, NodeFactory}
import org.slf4j.LoggerFactory

object GetSourceSummaryCliAction extends CliAction[List[Either[InvalidTable, TableHDFSInfo]]] {
  private val logger = LoggerFactory.getLogger(this.getClass)

  override def command: String = "get-source-summary"

  override def usageArgs: List[Any] = List.empty

  override def help: String = "Get summary of source tables"

  override def applyInternal(args: List[String], config: GlobalConfig): List[Either[InvalidTable, TableHDFSInfo]] = {
    val conf = config.backCompat
    logger.info("Getting the summary of source tables")
    val source = NodeFactory.getSourceFromConf(conf)
    val getSourceSummary = GetSourceSummaryAction(config, source)
    val summary = getSourceSummary()
    printSummary(summary)
    summary
  }

  def printSummary(summary: List[Either[InvalidTable, TableHDFSInfo]]): Unit = {
    println("Source tables summary")
    summary.foreach {
      case Left(InvalidTable(input, message)) =>
        println(s"$input is invalid, reason: $message")
      case Right(TableHDFSInfo(db, table, size, files, partitions)) =>
        println(s"$db.$table is available, size: $size Bytes, files: ${files.size}, partitions: $partitions")
    }
  }
}
