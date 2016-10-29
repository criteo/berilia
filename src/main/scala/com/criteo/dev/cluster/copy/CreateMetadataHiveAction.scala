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
    var strings: Array[String] = ddl.split("\n")

    //for some reason, hive's own 'show create table statement'
    //doesn't compile and needs to be fixed like below
    strings = strings.map(s => s.trim()).map(s => s.replace("(", " ("))

    //strip tblProperties, which do not seem to parse..
    var stbuffer: scala.collection.mutable.Buffer[String] = strings.toBuffer
    val tblPropertiesIndex = stbuffer.indexWhere(s => s.startsWith("TBLPROPERTIES"))
    stbuffer = stbuffer.dropRight(stbuffer.size - tblPropertiesIndex)

    //replace location string
    val locationIndex = stbuffer.map(s => s.trim()).indexOf("LOCATION")
    stbuffer.remove(locationIndex + 1)
    stbuffer.insert(locationIndex + 1, s"'${CopyUtilities.toRelative(location)}'")

    //handle pail format.  Use glupInputFormat to read it as a sequenceFile.
    //The other option is
    // 1.  Copy the pail.meta file in the table's root directory.
    // 2.  Set hive.input.format = com.criteo.hadoop.hive.ql.io.PailOrCombineHiveInputFormat on the target cluster.
//    val inputFormatIndex = stbuffer.map(s => s.trim()).indexOf("STORED AS INPUTFORMAT")
//    val isPailif =
//      stbuffer(inputFormatIndex + 1).toString().trim().contains("SequenceFileFormat$SequenceFilePailInputFormat")
//    if (isPailif) {
//      stbuffer.remove(inputFormatIndex + 1)
//      stbuffer.insert(inputFormatIndex + 1, "  'com.criteo.hadoop.hive.ql.io.GlupInputFormat'")
//    }

    return stbuffer.mkString(" ")
  }
}
