package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.{GeneralConstants, GeneralUtilities, Node}
import org.slf4j.LoggerFactory

/**
  * Gets the metadata from Hive.
  *
  * TODO- rewrite this using HiveMetaStoreClient for much better performance.
  */
class GetMetadataAction(conf : Map[String, String], node : Node, throttle: Boolean = true) {

  private val logger = LoggerFactory.getLogger(GetMetadataAction.getClass)

  def apply() = {
    logger.info("Getting source table metadata.")
    val stringList = GeneralUtilities.getConfStrict(conf, CopyConstants.sourceTables, GeneralConstants.sourceProps)
    val dbTables = stringList.split(";").map(s => s.trim())
    dbTables.map(s => getTableMetadata(s))
  }

  def getTableMetadata(dbTablePartSpec: String) : TableInfo = {
    //parse the configured source tables of form "$db.$table (part1=$part1, part2=$part2)"
    val regex = """(\S*)\.(\S*)\s*(.*)""".r

    dbTablePartSpec match {
      case regex(db, table, part) => {
        //1. Get the table metadata, like location, isPartitioned, and createStmt.
        val getTableMetadataAction = new GetTableMetadataAction(conf, node)
        val (isPartitioned, createStmt) = getTableMetadataAction(db, table)

        //2.  If partitioned, get the list of partitions.
        val partitionList: Array[String] =
          if (isPartitioned) {
            if (part.isEmpty) {
              ListPartitionAction(conf, node, db, table, None, throttle)
            } else {
              ListPartitionAction(conf, node, db, table, Some(part), throttle)
            }

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
      }
      case _ => throw new IllegalArgumentException(s"${CopyConstants.sourceTables}: $dbTablePartSpec")
    }
  }
}

object GetMetadataAction {
  def apply(conf: Map[String, String], node: Node, throttle: Boolean=true) = {
    val getMetadataAction = new GetMetadataAction(conf, node, throttle)
    getMetadataAction()
  }
}
