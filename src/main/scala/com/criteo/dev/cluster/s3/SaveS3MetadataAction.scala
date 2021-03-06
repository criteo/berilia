package com.criteo.dev.cluster.s3

import com.criteo.dev.cluster.Node
import com.criteo.dev.cluster.copy.{CopyUtilities, CreateMetadataAction, TableInfo}
import org.slf4j.LoggerFactory

/**
  * Saves 'metadata' in S3.
  *
  * Effectively, its just a file that is written to S3 with a list of Hive DDL's
  * that any dev cluster can run.
  *
  * It's marked by a timestamp, so that the nodes can run all the ddl's of a bucket
  * in sequence.
  */
class SaveS3MetadataAction(conf: Map[String, String], source: Node, target: Node) extends CreateMetadataAction (conf, target) {

  private val logger = LoggerFactory.getLogger(classOf[SaveS3MetadataAction])


  def apply(tableInfo: TableInfo): Unit = {

    val database = tableInfo.database
    val table = tableInfo.ddl.table

    //Create databases
    val createDb = s"create database if not exists $database;"

    //Create tables and add partitions.
    val newTableInfo = tableInfo.ddl.copy(
      location = Some(CopyUtilities.toLocationVar(tableInfo.ddl.location.get))
    )
    val createTable = newTableInfo.format.replaceAll("""\n""", " ")

    val partitionDdls =
      tableInfo.partitions.map(p => {
            s"alter table $table add if not exists " +
            s"partition (${CopyUtilities.partitionSpecString(p.partSpec, tableInfo.ddl.partitionedBy)}) " +
            s"location '${CopyUtilities.toLocationVar(p.location)}';"
        })

    val ddl = List(createDb, s"use $database", createTable) ++ partitionDdls
    logger.info("Uploading Hive DDL to S3 Bucket")
    UploadS3Action(conf, target, DataType.hive, ddl)
  }

  /**
    * More delicate parsing, to format the DDL so it creates the right table.
    *
    * @param ddl
    * @param location
    */
  def formatCreateDdl(tableInfo: TableInfo, ddl: String, location: String): String = {
    var strings: Array[String] = ddl.split("\n")

    //for some reason, hive's own 'show create table statement'
    //doesn't compile and needs to be fixed like below
    strings = strings.map(s => s.trim()).map(s => s.replace("(", " ("))

    //Also take opportunity to insert 'if not exists'
    var stbuffer: scala.collection.mutable.Buffer[String] = strings.toBuffer
    stbuffer.remove(0)
    stbuffer.insert(0, s"CREATE EXTERNAL TABLE IF NOT EXISTS `${tableInfo.database}.${tableInfo.ddl.table}` (")

    //strip tblProperties, which do not seem to parse..
    val tblPropertiesIndex = stbuffer.indexWhere(s => s.startsWith("TBLPROPERTIES"))
    stbuffer = stbuffer.dropRight(stbuffer.size - tblPropertiesIndex)

    //replace location string
    val locationIndex = stbuffer.map(s => s.trim()).indexOf("LOCATION")
    stbuffer.remove(locationIndex + 1)
    stbuffer.insert(locationIndex + 1, s"'${CopyUtilities.toLocationVar(location)}'")

    return stbuffer.mkString(" ", "", ";")
  }
}