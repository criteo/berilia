package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster._

/**
  * Creates Hive metadata on target.
  *
  * TODO- use HiveMetaStoreClient for better performance.
  */
class CreateMetadataHiveAction(conf: Map[String, String], node: Node) extends CreateMetadataAction(conf, node) {

  def apply(tableInfos: Array[TableInfo]): Unit = {
    val databases = tableInfos.map(ti => ti.database).distinct

    //Create databases
    val createDbs = databases.toList.map { d =>
      s"create database if not exists $d"
    }

    //Create tables and add partitions.
    val createTables = tableInfos.flatMap { ti =>
      val database = ti.database
      val table = ti.table

      val createDdl = formatCreateDdl(ti.createStmt, ti.location)
      val createPartition =
        if (ti.partitions.nonEmpty) {
          Some(ti.partitions.map(p => {
            s"partition (${CopyUtilities.partitionSpecString(p.partSpec)}) " +
              s"location '${CopyUtilities.toRelative(p.location)}' "
          }).mkString(s"alter table $table add ", "", ""))
        } else None

      Seq(s"use $database", createDdl) ++ createPartition
    }

    //To make it idempotent, do not fail if database or tables exist.
    SshHiveAction(node, createDbs ++ createTables, ignoreError = true)
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
