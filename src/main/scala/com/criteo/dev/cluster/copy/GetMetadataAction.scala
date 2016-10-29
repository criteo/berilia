package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.{GeneralConstants, GeneralUtilities, Node}
import org.slf4j.LoggerFactory

/**
  * Gets the metadata from Hive.
  *
  * TODO- rewrite this using HiveMetaStoreClient for much better performance.
  */
class GetMetadataAction(conf : Map[String, String], node : Node) {

  private val logger = LoggerFactory.getLogger(GetMetadataAction.getClass)

  def apply() = {
    logger.info("Getting source table metadata.")
    val dbTables = GeneralUtilities.getConfCSVStrict(conf, CopyConstants.sourceTables, GeneralConstants.sourceProps)
    dbTables.map(s => getTableMetadata(s))
  }

  def getTableMetadata(dbTable: String) : TableInfo = {
    dbTable.split('.').toList match {
      case db :: table :: Nil =>

        //1. Get the table metadata, like location, isPartitioned, and createStmt.
        val getTableMetadataAction = new GetTableMetadataAction(conf, node)
        val (isPartitioned, createStmt) = getTableMetadataAction(db, table)

        //2.  If partitioned, get the list of partitions.
        val partitionList: Array[String] =
          if (isPartitioned) {
            ListPartitionAction(conf, node, db, table)
          } else {
            Array.empty[String]
          }

        //3.  Get partitionSpec in model form.
        val partitionSpecList: Array[PartSpec] = partitionList.map(s => {
          CopyUtilities.getPartInfos(s)
        })

        //4.  Get partition locations as well
        val getPartitionAction = new GetPartitionMetadataAction(conf, node)
        val partitions = getPartitionAction(db, table, partitionSpecList)
        TableInfo(db,
          table,
          CopyUtilities.location(createStmt),
          CopyUtilities.inputFormat(createStmt),
          createStmt,
          partitions)
      case _ =>
        throw new IllegalArgumentException(s"${CopyConstants.sourceTables}: $dbTable")
    }
  }
}

object GetMetadataAction {
  def apply(conf: Map[String, String], node: Node) = {
    val getMetadataAction = new GetMetadataAction(conf, node)
    getMetadataAction()
  }
}
