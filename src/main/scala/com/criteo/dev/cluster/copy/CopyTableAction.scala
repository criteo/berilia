package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster._
import org.slf4j.LoggerFactory

/**
  * Copy a Hive table.  Either directs to sampling or full copy of specified number of partitions.
  */
class CopyTableAction(conf: Map[String, String], source: Node, target: Node) {

  private val logger = LoggerFactory.getLogger(classOf[CopyTableAction])

  def copy(tableInfo: TableInfo): Unit = {

    val sampleProbConf = CopyUtilities.getOverridableConf(conf,
      tableInfo.database,
      tableInfo.ddl.table,
      CopyConstants.sampleProb)
    val sampleProb = sampleProbConf.toDouble
    require(sampleProb > 0 && sampleProb <= 1, "Sample probability must be between 0 (exclusive) and 1 (inclusive)")

    val partitions = tableInfo.partitions
    val partLocations = partitions.map(_.location)

    if (sampleProb == 1 || underThreshold(partLocations)) {
      //sampling disabled for table, or size to copy less than configured sampling threshold, skip sampling.
      val fullCopy = new FullCopyTableAction(conf, source, target)
      fullCopy.copy(tableInfo)
    } else {
      //sample
      val sampleCopy = new SampleCopyTableAction(conf, source, target, sampleProb)
      sampleCopy.copy(tableInfo)
    }

    // Special handling
    fireEvents(conf, tableInfo)
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


  def underThreshold(sourceFiles: Array[String]) : Boolean = {
    val listAction = new SshMultiAction(NodeFactory.getSource(conf))
    sourceFiles.foreach(sf => {
      logger.info(s"Checking directory for need of sampling: $sf")
      listAction.add(s"hdfs dfs -ls -R $sf")
    })

    val sampleThresholdConf = GeneralUtilities.getConfStrict(conf, CopyConstants.sampleThreshold, GeneralConstants.sourceProps)
    val sampleThreshold = sampleThresholdConf.toLong
    val list = listAction.run(returnResult = true).split("\n")
    val totalSize = list.foldLeft(0L) ((sum, l) => {
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
