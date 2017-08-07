package com.criteo.dev.cluster.copy

import java.io.{File, PrintWriter}
import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, ZoneId}
import java.util.concurrent.atomic.AtomicReference
import java.util.function.UnaryOperator

import com.criteo.dev.cluster._
import com.criteo.dev.cluster.config.{Checkpoint, CheckpointWriter, GlobalConfig}
import com.criteo.dev.cluster.s3.{BucketUtilities, DataType, UploadS3Action}
import com.criteo.dev.cluster.source.{GetSourceSummaryAction, InvalidTable, SourceTableInfo}
import org.slf4j.LoggerFactory

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.util.{Failure, Success, Try}

/**
  * Utility that copies for kind of cluster (as long as configuration as source/target information)
  */
object CopyAllAction {

  private val logger = LoggerFactory.getLogger(CopyAllAction.getClass)
  type CopySuccess = (SourceTableInfo, Duration)
  type CopyFailure = (SourceTableInfo, Duration, Throwable)

  def apply(config: GlobalConfig, conf: Map[String, String], source: Node, target: Node) = {
    val sourceTables = GeneralUtilities.getNonEmptyConf(conf, CopyConstants.sourceTables)
    val sourceFiles = GeneralUtilities.getNonEmptyConf(conf, CopyConstants.sourceFiles)

    require((sourceTables.isDefined || sourceFiles.isDefined), "No source tables or files configured")

    logger.info(s"Copying with parallelism: ${config.source.parallelism}")
    //copy source tables from the checkpoint
    val begin = Instant.now
    val checkpoint = config.checkpoint match {
      case Some(c) =>
        logger.info(s"Copying from a checkpoint, todo: ${c.todo.size}, finished: ${c.finished.size}, failed: ${c.failed.size}")
        c
      case None =>
        logger.info(s"Copying all")
        Checkpoint(
          begin,
          begin,
          todo = config.source.tables.map(_.name).toSet
        )
    }
    val checkpointRef = new AtomicReference[Checkpoint](checkpoint)
    // get source table metadata and update the checkpoint
    val (invalid, valid) = GetSourceSummaryAction(config, source)(config.source.tables.filter(t => checkpoint.todo.contains(t.name))).partition(_.isLeft)
    checkpointRef.getAndUpdate(new UnaryOperator[Checkpoint] {
      override def apply(t: Checkpoint): Checkpoint = t.copy(
        updated = Instant.now,
        todo = valid.map(_.right.get.tableInfo.fullName).toSet,
        failed = invalid.map(_.left.get.name).toSet
      )
    })
    writeCheckpoint(checkpointRef.get)

    // parallel execution
    val parValidTables = valid.map(_.right.get).par
    parValidTables.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(config.source.parallelism))
    val results = parValidTables
      .map(sourceTableInfo => {
        val tableFullName = sourceTableInfo.tableInfo.fullName
        val start = Instant.now
        Try {
          // copy table
          val copyTableAction = new CopyTableAction(config, conf, source, target)
          val tt = copyTableAction.copy(sourceTableInfo)
          val createMetadataAction = CreateMetadataActionFactory.getCreateMetadataAction(config, conf, source, target)
          createMetadataAction(tt)
        } match {
          case Success(_) =>
            Left(sourceTableInfo -> Duration.between(start, Instant.now))
            val cp = checkpointRef.updateAndGet(new UnaryOperator[Checkpoint] {
              override def apply(t: Checkpoint): Checkpoint = t.copy(
                todo = t.todo - tableFullName,
                finished = t.finished + tableFullName
              )
            })
            writeCheckpoint(cp)
            Left(sourceTableInfo -> Duration.between(start, Instant.now))
          case Failure(e) =>
            logger.error(e.getMessage, e)
            val cp = checkpointRef.updateAndGet(new UnaryOperator[Checkpoint] {
              override def apply(t: Checkpoint): Checkpoint = t.copy(
                todo = t.todo - tableFullName,
                failed = t.failed + tableFullName
              )
            })
            writeCheckpoint(cp)
            Right((sourceTableInfo, Duration.between(start, Instant.now), e))
        }
      })
      .toList

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

    CleanupAction(source, target, config.source.isLocalScheme)
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

  def writeCheckpoint(checkpoint: Checkpoint): Unit = {
    val out = CheckpointWriter.render(checkpoint)
    val path = s"./checkpoint_${dateFormatter.format(checkpoint.created)}.json"
    val file = new File(path)
    try {
      logger.info(s"Writing checkpoint to $path")
      val pw = new PrintWriter(file)
      pw.print(out)
      pw.close()
      logger.info(s"checkpoint written to $path")
    } catch {
      case e: Throwable => logger.error(e.getMessage, e)
    }
  }

  private val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").withZone(ZoneId.systemDefault())
}
