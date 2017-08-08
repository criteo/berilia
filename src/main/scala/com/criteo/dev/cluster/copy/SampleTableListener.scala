package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.Node

/**
  * Any special handling needed for copy table.  For exmaple, copy over meta files, etc.
  *
  * A file copier is provided, with source and destination nodes.
  */
trait SampleTableListener {

  def onBeforeSample(
                      tableInfo: TableInfo,
                      sampleTableInfo: TableInfo,
                      source: Node,
                      isLocalScheme: Boolean
                    )
}
