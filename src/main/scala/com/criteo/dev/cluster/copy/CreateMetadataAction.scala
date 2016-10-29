package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.Node

/**
  * Most of the cases, we create the metadata of the copied tables on the cluster.
  *
  * In S3 case (shared data/metadata), we save it on S3 itself.
  */
abstract class CreateMetadataAction(conf: Map[String, String], target: Node) {

  def apply(tableInfos: Array[TableInfo]): Unit
}
