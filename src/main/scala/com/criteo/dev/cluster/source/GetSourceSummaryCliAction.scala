package com.criteo.dev.cluster.source

import java.io.{File, PrintWriter}

import com.criteo.dev.cluster.config.GlobalConfig
import com.criteo.dev.cluster.{CliAction, GeneralUtilities, NodeFactory}
import org.slf4j.LoggerFactory

object GetSourceSummaryCliAction extends CliAction[List[Either[InvalidTable, SourceTableInfo]]] {
  private val logger = LoggerFactory.getLogger(this.getClass)

  override def command: String = "get-source-summary"

  override def usageArgs: List[Any] = List.empty

  override def help: String = "Get summary of source tables"

  override def applyInternal(args: List[String], config: GlobalConfig): List[Either[InvalidTable, SourceTableInfo]] = {
    logger.info(s"Getting the summary of source tables, parallelism ${config.source.parallelism}")
    val source = NodeFactory.getSourceFromConf(config.source)
    val getSourceSummary = GetSourceSummaryAction(config, source)
    val summary = getSourceSummary(config.source.tables)
    printSummary(summary)
    exportToCSV(summary, s"${GeneralUtilities.getHomeDir}/source_summary_${GeneralUtilities.getSimpleDate}.csv")
    summary
  }

  def printSummary(summary: List[Either[InvalidTable, SourceTableInfo]]): Unit = {
    println("Source tables summary")
    val (invalid, valid) = summary.partition(_.isLeft)
    invalid.map(_.left.get).foreach { case InvalidTable(name, input, message) =>
      println(s"$name is invalid, input: $input, reason: $message")
    }
    valid.map(_.right.get) foreach { case SourceTableInfo(_, TableHDFSInfo(db, table, size, files, partitions)) =>
      println(s"$db.$table is available, size: $size Bytes, files: ${files.size}, partitions: $partitions")
    }
    val totalSize = valid.map(_.right.get.hdfsInfo.size).sum
    println(s"Invalid tables: ${invalid.size}, valid tables: ${valid.size}")
    println(s"Total size: $totalSize Bytes")
  }

  def exportToCSV(summary: List[Either[InvalidTable, SourceTableInfo]], filepath: String): Unit = {
    logger.info(s"writing source summary to $filepath")
    val file = new File(filepath)
    val printWriter = new PrintWriter(file)
    val headers = List("name", "bytes", "files", "partitions", "error")
    printWriter.println(headers.mkString(","))
    summary foreach {
      case Right(SourceTableInfo(table, TableHDFSInfo(_, _, size, files, partitions))) =>
        printWriter.println(List(table.fullName, size, files.size, partitions, "").mkString(","))
      case Left(InvalidTable(name, _, message)) =>
        printWriter.println(List(name, "", "", "", message).mkString(","))
    }
    printWriter.flush()
    printWriter.close()
    logger.info(s"source summary has been written to $filepath")
  }
}
