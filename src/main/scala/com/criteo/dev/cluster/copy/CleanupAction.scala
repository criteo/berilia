package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.{GeneralUtilities, Node}

/**
  * Cleanup the temp folders.
  */
object CleanupAction {

  def apply(conf: Map[String, String], source: Node, target: Node, isLocalScheme: Boolean = true): Unit = {
    if (isLocalScheme)
      GeneralUtilities.cleanupTempDir
    else
      CopyUtilities.deleteTmp(source, CopyConstants.tmpSrc)
    CopyUtilities.deleteTmp(target, CopyConstants.tmpTgt)
  }
}
