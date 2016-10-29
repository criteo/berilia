package com.criteo.dev.cluster.copy

/**
  * Cleanup the temp folders.
  */
object CleanupAction {

  def apply(conf: Map[String, String]): Unit = {
    CopyUtilities.deleteTmpSrc(conf)
    CopyUtilities.deleteTmpTgt(conf)
  }
}
