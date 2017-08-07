package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.{GeneralUtilities, Node}
import org.slf4j.LoggerFactory

/**
  * Cleanup the temp folders.
  */
object CleanupAction {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def apply(source: Node, target: Node, isLocalScheme: Boolean = false): Unit = {
    logger.info("Cleaning up temp directories")
    if (isLocalScheme) {
      logger.info(s"Deleting ${GeneralUtilities.getTempDir} of local")
      GeneralUtilities.cleanupTempDir
    } else {
      logger.info(s"Deleting ${CopyConstants.tmpSrc} of gateway")
      CopyUtilities.deleteTmp(source, CopyConstants.tmpSrc)
    }
    if (!isLocalScheme) {
      // Local copy action cleans up the target temp dir after each execution
      // TODO: use the same strategy for other schemes
      val targetDir = CopyConstants.tmpTgt
      logger.info(s"Deleting $targetDir of target")
      CopyUtilities.deleteTmp(target, targetDir)
    }
  }
}
