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
    val getMetadata = new GetMetadataAction(config, config.backCompat, target)
    val hiveScript = new SshHiveAction(target)
    val shellScript = new SshMultiAction(target)
    config.source.tables.filter(t => {
      if (t.skipCleanup) {
        logger.info(s"Skip ${t.name}")
        false
      } else true
    }).foreach(table =>
      Try {
        getMetadata(table.name, false)
      } match {
        case Success(t) => {
          t.ddl.location match {
            case Some(location) =>
              // remove file
              val command = s"hdfs dfs -rm -r -f $location"
              logger.info(s"Add HDFS command: $command")
              shellScript.add(command)
              // drop table
              val statement = s"DROP TABLE ${t.fullName}"
              logger.info(s"Add Hive statement: $statement")
              hiveScript.add(statement)
            case None =>
              logger.info(s"Cannot get the location of ${t.fullName}, skip cleaning")
          }
        }
        case Failure(e) => {
          logger.info(s"${table.name} is not valid, skip cleaning, message: ${e.getMessage}")
        }
      }
    )
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