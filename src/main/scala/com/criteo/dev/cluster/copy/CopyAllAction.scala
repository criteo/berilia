package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.s3.{DataType, UploadS3Action}
import com.criteo.dev.cluster.{GeneralConstants, GeneralUtilities, NodeFactory}
import org.slf4j.LoggerFactory

/**
  * Utility that copies for kind of cluster (as long as configuration as source/target information)
  */
object CopyAllAction {

  private val logger = LoggerFactory.getLogger(CopyAllAction.getClass)

  def apply(conf: Map[String, String]) = {
    val sourceTables = GeneralUtilities.getNonEmptyConf(conf, CopyConstants.sourceTables)
    val sourceFiles = GeneralUtilities.getNonEmptyConf(conf, CopyConstants.sourceFiles)

    val source = NodeFactory.getSource(conf)
    val target = NodeFactory.getTarget(conf)

    require ((sourceTables.isDefined || sourceFiles.isDefined),
      "No source tables or files configured")

    //copy source tables
    if (sourceTables.isDefined) {
      val tableInfos: Array[TableInfo] = GetMetadataAction(conf, source)
      tableInfos.foreach(ti => {
        val copyTableAction = new CopyTableAction(conf, source, target)
        copyTableAction.copy(ti)
      })
      val createMetadataAction = CreateMetadataActionFactory.getCopyFileAction(conf, target)
      createMetadataAction(tableInfos)
    }

    //copy files
    if (sourceFiles.isDefined) {
      val files = GeneralUtilities.getConfCSV(conf, CopyConstants.sourceFiles)
      val copyFileAction = CopyFileActionFactory.getCopyFileAction(conf, source, target)
      files.foreach(f => copyFileAction(Array(f), f, CopyUtilities.toRelative(f)))

      //write in the logs
      val targetType = GeneralUtilities.getConfStrict(conf, GeneralConstants.targetTypeProp, "Internally-provided")
      if (targetType.equals(GeneralConstants.s3Type)) {
        UploadS3Action(conf, NodeFactory.getTarget(conf), DataType.hdfs, files.toList.map(
          f => CopyUtilities.toS3BucketTarget(conf, f, includeCredentials=false)))
      }
    }

    logger.info("Cleaning up temp directories")
    CleanupAction(conf)
  }
}
