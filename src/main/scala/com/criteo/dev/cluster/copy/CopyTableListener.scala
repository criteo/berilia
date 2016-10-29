package com.criteo.dev.cluster.copy

/**
  * Any special handling needed for copy table.  For exmaple, copy over meta files, etc.
  *
  * A file copier is provided, as is source and destination nodes.
  */
trait CopyTableListener {

  def onCopy(tableInfo: TableInfo, copyFileAction: CopyFileAction)
}
