package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.Node
import org.slf4j.LoggerFactory

/**
  * Copies over pail.meta file.
  */
class PailCopyListener extends CopyTableListener {

  private val logger = LoggerFactory.getLogger(classOf[PailCopyListener])

  override def onCopy(tableInfo: TableInfo, copyFileAction: CopyFileAction) : Unit = {
    if (CopyUtilities.inputFormat(tableInfo.createStmt)
      .contains("SequenceFileFormat$SequenceFilePailInputFormat")) {

      val tableLocation = tableInfo.location
      logger.info(s"Special handling for pail format, copying file: $tableLocation/pail.meta")
      copyFileAction(Array(s"$tableLocation/pail.meta"),
        tableLocation,
        CopyUtilities.toRelative(tableLocation))
    }
  }
}
