package com.criteo.dev.cluster.copy

import java.time.{Duration, Instant}

import com.criteo.dev.cluster.s3.{BucketUtilities, DataType, UploadS3Action}
import com.criteo.dev.cluster._
import com.criteo.dev.cluster.config.GlobalConfig
import com.criteo.dev.cluster.source.{GetSourceSummaryAction, InvalidTable, SourceTableInfo}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * Utility that copies for kind of cluster (as long as configuration as source/target information)
  */
object CopyAllAction {

  private val logger = LoggerFactory.getLogger(CopyAllAction.getClass)

  def apply(config: GlobalConfig, conf: Map[String, String], source: Node, target: Node) = {
    val sourceTables = GeneralUtilities.getNonEmptyConf(conf, CopyConstants.sourceTables)
    val sourceFiles = GeneralUtilities.getNonEmptyConf(conf, CopyConstants.sourceFiles)

    require((sourceTables.isDefined || sourceFiles.isDefined),
      "No source tables or files configured")

    //copy source tables
    val begin = Instant.now
    val (invalid, valid) = GetSourceSummaryAction(config, source)(config.source.tables).partition(_.isLeft)
    val results = valid
      .map(_.right.get)
      .map { sourceTableInfo =>
        val start = Instant.now
        Try {
          val copyTableAction = new CopyTableAction(config, conf, source, target)
          val tt = copyTableAction.copy(sourceTableInfo)
          val createMetadataAction = CreateMetadataActionFactory.getCreateMetadataAction(conf, source, target)
          createMetadataAction(tt)
        } match {
          case Success(_) =>
            Left((sourceTableInfo, Duration.between(start, Instant.now)))
          case Failure(e) =>
            Right((sourceTableInfo, Duration.between(start, Instant.now), e))
        }
      }
    printCopyTableResult(begin, invalid.map(_.left.get), results)

    //copy files
    if (sourceFiles.isDefined) {
      val files = GeneralUtilities.getConfCSV(conf, CopyConstants.sourceFiles)
      val copyFileAction = CopyFileActionFactory.getCopyFileAction(conf, source, target)
      files.foreach(f => copyFileAction(Array(f), f, CopyUtilities.toRelative(f)))

      //write in the logs
      val targetType = GeneralUtilities.getConfStrict(conf, GeneralConstants.targetTypeProp, "Internally-provided")
      if (targetType.equals(GeneralConstants.s3Type)) {
        UploadS3Action(conf, target, DataType.hdfs, files.toList.map(
          f => BucketUtilities.toS3Location(conf, target.ip, f, NodeType.AWS, includeCredentials = false)))
      }
    }

    logger.info("Cleaning up temp directories")
    CleanupAction(conf, source, target)
  }

  def printCopyTableResult(
                     start: Instant,
                     invalidTables: List[InvalidTable],
                     copyResult: List[Either[(SourceTableInfo, Duration), (SourceTableInfo, Duration, Throwable)]]
                   ) = {
    invalidTables.foreach { i =>
      logger.info(s"Skipped copying ${i.input} ${i.message}")
    }
    copyResult foreach {
      case Left((source, duration)) =>
        logger.info(
          s"Copying of ${source.tableInfo.database}.${source.tableInfo.name} has succeeded, duration: ${duration.getSeconds} seconds"
        )
      case Right((source, duration, e)) =>
        logger.debug(e.getMessage, e)
        logger.info(
          s"Copying of ${source.tableInfo.database}.${source.tableInfo.name} has failed, cause: ${e.getMessage}, duration: ${duration.getSeconds} seconds"
        )
    }
    logger.info(
      s"Data copy finished, skipped: ${invalidTables.size}, success: ${copyResult.filter(_.isLeft).size}, failure: ${copyResult.filter(_.isRight).size}"
    )
    logger.info(s"Total time elapsed ${Duration.between(start, Instant.now).getSeconds} seconds")
  }
}
