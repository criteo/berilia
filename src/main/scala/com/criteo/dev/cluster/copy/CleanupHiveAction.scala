package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.Node
import com.criteo.dev.cluster.command.{SshHiveAction, SshMultiAction}
import com.criteo.dev.cluster.config.GlobalConfig
import com.criteo.dev.cluster.source.{GetSourceMetadataAction, HDFSFileInfo, FullTableInfo}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object CleanupHiveAction {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def apply(config: GlobalConfig, target: Node): Try[CleanResult] = {
    val action = GetSourceMetadataAction(config, target)
    val hiveScript = new SshHiveAction(target)
    val shellScript = new SshMultiAction(target)
    action(
      config.source.tables.filter(t => {
        if (t.skipCleanup) {
          logger.info(s"Skip ${t.name}")
          false
        } else true
      }),
      useLocalScheme = false
    ) map {
      case Left(invalid) =>
        logger.info(s"${invalid.name} is invalid, skip cleaning")
      case Right(FullTableInfo(tableInfo, hdfsInfo)) =>
        logger.info(s"Cleaning up ${tableInfo.fullName}")
        // remove tables/partitions
        val partitions = tableInfo.partitions.map { part =>
          s"PARTITION (${CopyUtilities.partitionSpecString(part.partSpec, tableInfo.ddl.partitionedBy)})"
        }.mkString(", ")
        val statement = if (partitions.isEmpty)
          s"DROP TABLE ${tableInfo.fullName}"
        else
          s"ALTER TABLE ${tableInfo.fullName} DROP $partitions"
        logger.info(s"Add Hive statement: $statement")
        hiveScript.add(statement)
        // remove files
        hdfsInfo.files.foreach { case HDFSFileInfo(path, _) =>
          val command = s"hdfs dfs -rm -r $path"
          logger.info(s"Add HDFS command: $command")
          shellScript.add(command)
        }
    }
    val res = for {
      hive <- Try(hiveScript.run())
      shell <- Try(shellScript.run())
    } yield (hive, shell)
    res match {
      case Success((hive, shell)) =>
        Success(CleanResult(hive, shell))
      case Failure(e) =>
        logger.error(e.getMessage, e)
        Failure(e)
    }
  }
}

case class CleanResult(
                        hive: String,
                        hdfs: String
                      )