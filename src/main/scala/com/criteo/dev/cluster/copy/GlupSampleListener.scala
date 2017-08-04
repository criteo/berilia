package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.utils.ddl.{IOFormat, ParserConstants, SerDe}
import com.criteo.dev.cluster.Node
import com.criteo.dev.cluster.command.{ShellHiveAction, SshHiveAction}
import org.slf4j.LoggerFactory

import scala.util.Try
import scala.util.control.NonFatal

/**
  * For sampling tables of this input format, we should preserve the data format, so hence force it to be the same
  * output format.
  */
class GlupSampleListener extends SampleTableListener {

  private val logger = LoggerFactory.getLogger(classOf[GlupSampleListener])

  def originalInput = "com.criteo.hadoop.hive.ql.io.GlupInputFormat"

  def finalInput = "com.criteo.hadoop.hive.ql.io.GlupInputFormat"

  def originalOutput = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

  def finalOutput = "com.criteo.hadoop.hive.ql.io.GlupOutputFormat"

  override def onBeforeSample(
                               tableInfo: TableInfo,
                               sampleTableInfo: TableInfo,
                               source: Node,
                               isLocalScheme: Boolean
                             ) = {

    sampleTableInfo.ddl.storageFormat match {
      case (Some(io: IOFormat)) => {
        if (io.input.contains(originalInput) && io.output.contains(originalOutput)) {

          val alterStmtBase = s"alter table ${sampleTableInfo.ddl.table} "
          val setFormatBase = s"set fileformat inputformat '$finalInput' " +
            s"outputformat '$finalOutput'"

          Try {
            val alterTableAction = if (isLocalScheme) new ShellHiveAction else new SshHiveAction(source)
            alterTableAction.add(s"use ${sampleTableInfo.database}")
            alterTableAction.add(alterStmtBase + setFormatBase)

//            sampleTableInfo.partitions.foreach(p => {
//              val alterPartSb = new StringBuilder(alterStmtBase)
//              alterPartSb.append(s"partition (${
//                CopyUtilities.partitionSpecString(p.partSpec,
//                  sampleTableInfo.ddl.partitionedBy)
//              }) ")
//              alterPartSb.append(setFormatBase)
//              alterTableAction.add(alterPartSb.toString)
//            })

            alterTableAction.run()
          } recoverWith { case NonFatal(e) =>
            //CDH5+ Hive version needs a 'serde' keyword.
            logger.info("Unable to parse old Hive alter table statement, falling back to CDH5+ version")

            Try {
              val addendum = sampleTableInfo.ddl.rowFormat match {
                case Some(s: SerDe) => " serde " + s.format.stripPrefix(s"${ParserConstants.rowFormatSerde}")
                case _ => ""
              }

              val alterTableAction = if (isLocalScheme) new ShellHiveAction else new SshHiveAction(source)
              alterTableAction.add(s"use ${sampleTableInfo.database}")
              alterTableAction.add(alterStmtBase + setFormatBase + addendum)

//              sampleTableInfo.partitions.foreach(p => {
//                val alterPartSb = new StringBuilder(alterStmtBase)
//                alterPartSb.append(s"partition (${
//                  CopyUtilities.partitionSpecString(p.partSpec,
//                    sampleTableInfo.ddl.partitionedBy)
//                }) ")
//                alterPartSb.append(setFormatBase)
//                alterPartSb.append(addendum)
//                alterTableAction.add(alterPartSb.toString)
//              })
              alterTableAction.run()
            }
          }
        }
      }
      case _ =>
    }
  }
}
