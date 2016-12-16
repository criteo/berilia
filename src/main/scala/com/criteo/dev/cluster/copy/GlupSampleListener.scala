package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.utils.ddl.{IOFormat, ParserConstants, SerDe}
import com.criteo.dev.cluster.{Node, SshHiveAction}

/**
  * For sampling tables of this input format, we should preserve the data format, so hence force it to be the same
  * output format.
  */
class GlupSampleListener extends SampleTableListener {

  def originalInput = "com.criteo.hadoop.hive.ql.io.GlupInputFormat"
  def finalInput = "org.apache.hadoop.mapred.TextInputFormat"

  def originalOutput = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
  def finalOutput = "com.criteo.hadoop.hive.ql.io.GlupOutputFormat"

  override def onBeforeSample(tableInfo: TableInfo,
                     sampleTableInfo: TableInfo,
                     source: Node) = {

    sampleTableInfo.ddl.storageFormat match {
      case (Some(io : IOFormat)) => {
        if (io.input.contains(originalInput) && io.output.contains(originalOutput)) {
          val alterTableAction = new SshHiveAction(source)
          alterTableAction.add(s"use ${sampleTableInfo.database}")
          val alterTableStmt = new StringBuilder(s"alter table ${sampleTableInfo.ddl.table} " +
            s"set fileformat inputformat '$finalInput' " +
            s"outputformat '$finalOutput'")
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
