package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.Node
import com.criteo.dev.cluster.utils.ddl.{Delimited, IOFormat, SerDe}
import org.slf4j.LoggerFactory

/**
  * Copies over pail.meta file.
  */
class PailCopyListener extends CopyTableListener {

  private val logger = LoggerFactory.getLogger(classOf[PailCopyListener])

  override def onCopy(tableInfo: TableInfo, copyFileAction: CopyFileAction, source: Node, target: Node) : Unit = {
    tableInfo.ddl.storageFormat match {
      case Some(io: IOFormat) =>
        if (io.input.equals("SequenceFileFormat$SequenceFilePailInputFormat")) {
          val tableLocation = tableInfo.ddl.location.get
          logger.info(s"Special handling for pail format, copying file: $tableLocation/pail.meta")
          copyFileAction(Array(s"$tableLocation/pail.meta"),
            tableLocation,
            CopyUtilities.toRelative(tableLocation))
        }
      case _ =>
    }
  }
}
