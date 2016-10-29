package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster._
import com.criteo.dev.cluster.s3.ScpCopyFileAction
import org.slf4j.LoggerFactory

import scala.sys.process.ProcessLogger

/**
  * Copy over table data.
  */
class CopyTableAction (conf: Map[String, String]) {

  private val logger = LoggerFactory.getLogger(classOf[ScpCopyFileAction])
  private val processLogger = ProcessLogger(
    (e: String) => logger.info("err " + e))

  val source = NodeFactory.getSource(conf)
  val target = NodeFactory.getTarget(conf)

  def apply(tableInfo: TableInfo): Unit = {
    val database = tableInfo.database
    val table = tableInfo.table
    val location = tableInfo.location
    val partitions = tableInfo.partitions

    logger.info("Copying " + partitions.length + " partitions from " +
      database + "." + table)

    //for now, only support table location as the common location.
    val sourceCommon = getCommonLocation(location, partitions)

    //handle case of no partitions, and some partitions.
    val sourceLocations: Array[String] = {
      if (partitions.isEmpty) Array(location)
      else partitions.map(_.location)
    }

    val copyFileAction = CopyFileActionFactory.getCopyFileAction(conf)
    copyFileAction(sourceLocations, sourceCommon, CopyUtilities.toRelative(sourceCommon))

    // Special handling
    fireEvents(tableInfo)
  }


  def fireEvents(tableInfo: TableInfo) = {
    val listeners = GeneralUtilities.getNonEmptyConf(conf, CopyConstants.copyListeners)
    if (listeners.isDefined) {
      listeners.get.split(",").map(_.trim()).foreach(l => {
        val clazz = this.getClass.getClassLoader.loadClass(l)
        val listener = clazz.newInstance().asInstanceOf[CopyTableListener]
        val copyFileAction = CopyFileActionFactory.getCopyFileAction(conf)
        listener.onCopy(tableInfo, copyFileAction)
      })
    }
  }

  private def getCommonLocation(tableLocation: String, partitionInfos: Array[PartitionInfo]): String = {
    if (partitionInfos.isEmpty) {
      tableLocation
    } else {
      val partLocations = partitionInfos.map(_.location)
      // getCommonLocation(partLocations)

      partitionInfos.foreach(p => {
        //TODO - support this case, find the common parent of partitions if not under table directory,
        //will work with the following code.
        require(p.location.startsWith(tableLocation))
      })
      tableLocation
    }
  }

//  def getCommonLocation(partLocation: Array[String]): String = {
//    val location = partLocation.reduce[String] { case (prefix, cur) =>
//      prefix.zip(cur).takeWhile { case (a, b) => a == b }.map(_._1).mkString
//    }
//
//    CopyUtilities.getParent(location)
//  }
}

object CopyTableAction {

  def apply(conf: Map[String, String], tableInfo: TableInfo) = {
    val copyFileAction = new CopyTableAction(conf)
    copyFileAction.apply(tableInfo)
  }
}