package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster._
import com.criteo.dev.cluster.utils.ddl.CreateTable

/**
  * Creates Hive metadata on target.
  *
  * TODO- use HiveMetaStoreClient for better performance.
  */
class CreateMetadataHiveAction(conf: Map[String, String], node: Node) extends CreateMetadataAction(conf, node) {

  def apply(tableInfo: TableInfo): Unit = {

    val database = tableInfo.database
    val table = tableInfo.ddl.table

    //Create databases
    val createDb = s"create database if not exists $database"

    //Create tables
    val newTable = tableInfo.ddl.copy(location = Some(CopyUtilities.toRelative(tableInfo.ddl.location.get)))
    val newTableDdl = newTable.format

    //Add partitions
    val createPartitions =
      if (tableInfo.partitions.nonEmpty) {
        Some(tableInfo.partitions.map(p => {
          s"partition (${CopyUtilities.partitionSpecString(p.partSpec, newTable.partitionedBy)}) " +
            s"location '${CopyUtilities.toRelative(p.location)}' "
        }).mkString(s"alter table $table add ", "", ""))
      } else None

    //To make it idempotent, do not fail if database or tables exist.
    if (createPartitions.isDefined) {
      SshHiveAction(node, List(createDb, s"use $database", newTableDdl, createPartitions.get), ignoreError = true)
    } else {
      SshHiveAction(node, List(createDb, s"use $database", newTableDdl), ignoreError = true)
    }
  }
}
