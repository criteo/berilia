package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster._
import com.criteo.dev.cluster.config.GlobalConfig
import com.criteo.dev.cluster.source.FullTableInfo
import org.slf4j.LoggerFactory

/**
  * Copy a Hive table.  Either directs to sampling or full copy of specified number of partitions.
  *
  * @param config The configuration
  * @param conf   The configuration in old format, used by CopyFileAction, should be deprecated
  * @param source The source node
  * @param target The target node
  */
class CopyTableAction(config: GlobalConfig, conf: Map[String, String], source: Node, target: Node) {

  private val logger = LoggerFactory.getLogger(classOf[CopyTableAction])

  /**
    * @param sourceTableInfo source table info
    * @return target table info
    */
  def copy(sourceTableInfo: FullTableInfo): TableInfo = {
    val tableInfo = sourceTableInfo.tableInfo
    val hdfsInfo = sourceTableInfo.hdfsInfo
    val tableName = s"${tableInfo.database}.${tableInfo.name}"
    config.app.tables
      .find(_.name == tableName)
      .map { t =>
        t.sampleSize.map(_.toDouble / hdfsInfo.size)
          .orElse(t.sampleProb)
          .orElse(config.app.defaultSampleSize.map(_.toDouble / hdfsInfo.size))
          .getOrElse(config.app.defaultSampleProb)
      }
      .map { sampleProb =>
        val res: TableInfo = if (sampleProb >= 1 || hdfsInfo.size <= config.app.copyConfig.sampleThreshold) {
          logger.info(s"Copying $tableName without sampling ")
          new FullCopyTableAction(config, conf, source, target).copy(tableInfo)
        } else {
          logger.info(s"Copying $tableName with sample prob $sampleProb")
          new SampleCopyTableAction(config, conf, source, target, sampleProb).copy(tableInfo)
        }
        // Special handling
        fireEvents(conf, tableInfo)
        res
      }
      .get
  }

  def fireEvents(conf: Map[String, String], tableInfo: TableInfo) = {
    val listeners = GeneralUtilities.getNonEmptyConf(conf, CopyConstants.copyListeners)
    if (listeners.isDefined) {
      listeners.get.split(",").map(_.trim()).foreach(l => {
        val clazz = this.getClass.getClassLoader.loadClass(l)
        val listener = clazz.newInstance().asInstanceOf[CopyTableListener]
        val copyFileAction = CopyFileActionFactory.getCopyFileAction(config, source, target)
        listener.onCopy(tableInfo, copyFileAction, source, target)
      })
    }
  }
}
