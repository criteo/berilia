package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster._
import com.criteo.dev.cluster.config.GlobalConfig
import com.criteo.dev.cluster.source.SourceTableInfo
import org.slf4j.LoggerFactory

/**
  * Copy a Hive table.  Either directs to sampling or full copy of specified number of partitions.
  * @param config The configuration
  * @param conf The configuration in old format, used by CopyFileAction, should be deprecated
  * @param source The source node
  * @param target The target node
  */
class CopyTableAction(config: GlobalConfig, conf: Map[String, String], source: Node, target: Node) {

  private val logger = LoggerFactory.getLogger(classOf[CopyTableAction])

  /**
    * @param tableInfo source table info
    * @return target table info
    */
  def copy(sourceTableInfo: SourceTableInfo): TableInfo = {
    val tableInfo = sourceTableInfo.tableInfo
    val hdfsInfo = sourceTableInfo.hdfsInfo
    val tableName = s"${tableInfo.database}.${tableInfo.name}"
    config.source.tables
      .find(_.name == tableName)
      .map { t =>
        t.sampleSize.map(_.toDouble / hdfsInfo.size).orElse(t.sampleProb).getOrElse(config.source.defaultSampleProb)
      }
      .map { sampleProb =>
        val partitions = tableInfo.partitions
        val partLocations = partitions.map(_.location)
        val res: TableInfo = if (sampleProb >= 1 || underThreshold(partLocations)) {
          logger.info(s"Copying $tableName without sampling ")
          new FullCopyTableAction(conf, source, target).copy(tableInfo)
        } else {
          logger.info(s"Copying $tableName with sample prob $sampleProb")
          new SampleCopyTableAction(conf, source, target, sampleProb).copy(tableInfo)
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
        val copyFileAction = CopyFileActionFactory.getCopyFileAction(conf, source, target)
        listener.onCopy(tableInfo, copyFileAction, source, target)
      })
    }
  }


  def underThreshold(sourceFiles: Array[String]): Boolean = {
    val listAction = new SshMultiAction(source)
    sourceFiles.foreach(sf => {
      logger.info(s"Checking directory for need of sampling: $sf")
      listAction.add(s"hdfs dfs -ls -R $sf")
    })

    val sampleThresholdConf = GeneralUtilities.getConfStrict(conf, CopyConstants.sampleThreshold, GeneralConstants.sourceProps)
    val sampleThreshold = sampleThresholdConf.toLong
    val list = listAction.run(returnResult = true).split("\n")
    val totalSize = list.foldLeft(0L)((sum, l) => {
      val fileInfo = l.split("\\s+")
      if (fileInfo.length > 7) {
        val fileName = fileInfo(7)
        val size = fileInfo(4)
        logger.info(s"Size=$size, file=$fileName")
        sum + Integer.valueOf(size)
      } else {
        logger.info(s"Unexpected file information: $l")
        0
      }
    })

    logger.info(s"Total size of files to be copied = $totalSize")
    if (totalSize < sampleThreshold) {
      logger.info("Skipping sampling due to small file size.")
      return true
    }
    false
  }
}
