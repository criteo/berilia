package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.utils.ddl.{IOFormat, ParserConstants, SerDe}
import com.criteo.dev.cluster.{Node, SshHiveAction}

/**
  * For sampling tables of this input format, we should preserve the data format, so hence force it to be the same
  * output format.
  */
class GlupSampleListener extends SampleTableListener {

  def inputFormat = "com.criteo.hadoop.hive.ql.io.GlupInputFormat"
  def outputFormat = "com.criteo.hadoop.hive.ql.io.GlupOutputFormat"
  //def outputFormat = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

  override def onBeforeSample(tableInfo: TableInfo,
                     sampleTableInfo: TableInfo,
                     source: Node) = {

    sampleTableInfo.ddl.storageFormat match {
      case (Some(io : IOFormat)) => {
        if (io.input.contains(inputFormat)) {
          val alterTableAction = new SshHiveAction(source)
          alterTableAction.add(s"use ${sampleTableInfo.database}")
          val alterTableStmt = new StringBuilder(s"alter table ${sampleTableInfo.ddl.table} " +
            s"set fileformat inputformat '$inputFormat' " +
            s"outputformat '$outputFormat'")
          sampleTableInfo.ddl.rowFormat match {
            case Some(s: SerDe) => {
              alterTableStmt.append(s" serde ")
              alterTableStmt.append(s.format.stripPrefix(s"${ParserConstants.rowFormatSerde}"))
            }
            case _ =>
          }
          alterTableAction.add(alterTableStmt.toString)
          alterTableAction.run()
        }
      }
      case _ =>
    }
  }
}
