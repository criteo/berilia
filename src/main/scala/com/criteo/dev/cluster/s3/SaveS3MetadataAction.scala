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
class SaveS3MetadataAction(conf: Map[String, String], target: Node) extends CreateMetadataAction (conf, target) {

  private val logger = LoggerFactory.getLogger(classOf[SaveS3MetadataAction])


  def apply(tableInfos: Array[TableInfo]): Unit = {
    val databases = tableInfos.map(ti => ti.database).distinct

    //Create databases
    val createDbs = databases.toList.map { d =>
      s"create database if not exists $d;"
    }

    //Create tables and add partitions.
    val createTables = tableInfos.flatMap(ti => {
      val database = ti.database
      val table = ti.table

      val tableDdl = Seq(s"use $database;", formatCreateDdl(ti, ti.createStmt, ti.location))

      val partitionDdls =
        ti.partitions.map(p => {
            s"alter table $table add if not exists " +
            s"partition (${CopyUtilities.partitionSpecString(p.partSpec)}) " +
            s"location '${toS3Location(conf, p.location)}';"
        })

      tableDdl ++ partitionDdls
    })

    val ddl = createDbs ++ createTables
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
    stbuffer.insert(0, s"CREATE EXTERNAL TABLE IF NOT EXISTS `${tableInfo.database}.${tableInfo.table}` (")

    //strip tblProperties, which do not seem to parse..
    val tblPropertiesIndex = stbuffer.indexWhere(s => s.startsWith("TBLPROPERTIES"))
    stbuffer = stbuffer.dropRight(stbuffer.size - tblPropertiesIndex)

    //replace location string
    val locationIndex = stbuffer.map(s => s.trim()).indexOf("LOCATION")
    stbuffer.remove(locationIndex + 1)
    stbuffer.insert(locationIndex + 1, s"'${toS3Location(conf, location)}'")

    return stbuffer.mkString(" ", "", ";")
  }

  def toS3Location(conf: Map[String, String], sourceLocation: String) = {
    val relativeLocation = CopyUtilities.toRelative(sourceLocation)
    CopyUtilities.toS3BucketTarget(conf, relativeLocation, includeCredentials = false)
  }
}