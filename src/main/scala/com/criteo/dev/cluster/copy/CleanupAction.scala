package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.Node

/**
  * Cleanup the temp folders.
  */
object CleanupAction {

  def apply(conf: Map[String, String], source: Node, target: Node): Unit = {
    CopyUtilities.deleteTmp(source, CopyConstants.tmpSrc)
    CopyUtilities.deleteTmp(target, CopyConstants.tmpTgt)
  }
}
