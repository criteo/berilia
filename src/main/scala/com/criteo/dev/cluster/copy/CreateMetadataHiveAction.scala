package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster._

/**
  * Creates Hive metadata on target.
  *
  * TODO- use HiveMetaStoreClient for better performance.
  */
class CreateMetadataHiveAction(conf: Map[String, String], node: Node) extends CreateMetadataAction(conf, node) {

  def apply(tableInfo: TableInfo): Unit = {

    val database = tableInfo.database
    val table = tableInfo.table

    //Create databases
    val createDb = s"create database if not exists ${tableInfo.database}"

    //Create tables and add partitions.
    val createDdl = formatCreateDdl(tableInfo.createStmt, tableInfo.location)

    val createPartitions =
      if (tableInfo.partitions.nonEmpty) {
        Some(tableInfo.partitions.map(p => {
          s"partition (${CopyUtilities.partitionSpecString(p.partSpec)}) " +
            s"location '${CopyUtilities.toRelative(p.location)}' "
        }).mkString(s"alter table $table add ", "", ""))
      } else None

    //To make it idempotent, do not fail if database or tables exist.
    if (createPartitions.isDefined) {
      SshHiveAction(node, List(createDb, s"use $database", createDdl, createPartitions.get), ignoreError = true)
    } else {
      SshHiveAction(node, List(createDb, s"use $database", createDdl), ignoreError = true)
    }
  }

  /**
    * More delicate parsing, to format the DDL so it creates the right table.
    *
    * @param ddl
    * @param location
    */
  def formatCreateDdl(ddl: String, location: String): String = {
    CopyUtilities.formatDdl(ddl, tableName = None, location = Some(CopyUtilities.toRelative(location)))
  }
}
