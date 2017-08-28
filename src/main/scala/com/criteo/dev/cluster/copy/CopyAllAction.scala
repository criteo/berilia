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
import com.typesafe.config.ConfigRenderOptions
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
    val checkpoint: Checkpoint = config.checkpoint match {
      case Some(c) =>
        logger.info(
          s"Copying from an existing checkpoint, todo: ${c.todo.size}, finished: ${c.finished.size}, failed: ${c.failed.size}, invalid: ${c.invalid.size}"
        )
        // if new tables are defined in source but not in the checkpoint, include them in "todo"
        val newEntries = config.source.tables.map(_.name).toSet -- c.finished -- c.failed -- c.invalid
        logger.info(s"Adding ${newEntries.size} new entries to todo")
        c.copy(todo = c.todo ++ newEntries)
      case None =>
        logger.info(s"Copying with a new checkpoint")
        Checkpoint(
          begin,
          begin,
          todo = config.source.tables.map(_.name).toSet
        )
    }
    val checkpointRef = new AtomicReference[Checkpoint](checkpoint)
    // get source table metadata and update the checkpoint
    logger.info(s"Adding ${checkpoint.failed.size} failed entries to todo for retry")
    val (invalid, valid) = GetSourceSummaryAction(config, source)(
      config.source.tables.filter(t => checkpoint.todo.contains(t.name) || checkpoint.failed.contains(t.name))
    ).partition(_.isLeft)
    checkpointRef.getAndUpdate(new UnaryOperator[Checkpoint] {
      override def apply(t: Checkpoint): Checkpoint = t.copy(
        updated = Instant.now,
        todo = valid.map(_.right.get.tableInfo.fullName).toSet,
        failed = Set.empty,
        invalid = invalid.map(_.left.get.name).toSet ++ checkpoint.invalid
      )
    })
    writeCheckpoint(checkpointRef.get)

    // parallel execution
    val parValidTables = valid.map(_.right.get).par
    parValidTables.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(config.source.parallelism.table))
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
            val cp = checkpointRef.updateAndGet(new UnaryOperator[Checkpoint] {
              override def apply(t: Checkpoint): Checkpoint = t.copy(
                updated = Instant.now,
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
                updated = Instant.now,
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
      val copyFileAction = CopyFileActionFactory.getCopyFileAction(config, source, target)
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
      s"Data copy finished, invalid: ${invalidTables.size}, success: ${copyResult.filter(_.isLeft).size}, failure: ${copyResult.filter(_.isRight).size}"
    )
    logger.info(s"Total time elapsed: ${Duration.between(start, Instant.now).getSeconds} seconds")
  }

  def writeCheckpoint(checkpoint: Checkpoint): Unit = this.synchronized {
    val out = CheckpointWriter.render(checkpoint)
    val path = s"${GeneralUtilities.getHomeDir}/checkpoint_${dateFormatter.format(checkpoint.created)}.conf"
    val file = new File(path)
    try {
      logger.info(s"Writing checkpoint to $path")
      val pw = new PrintWriter(file)
      pw.print(out)
      pw.close()
      logger.info(s"Checkpoint written to $path")
    } catch {
      case e: Throwable => logger.error(e.getMessage, e)
    }
  }

  private val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").withZone(ZoneId.systemDefault())
}
