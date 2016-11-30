package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.{GeneralConstants, GeneralUtilities, Node}
import org.slf4j.LoggerFactory

/**
  * Gets the metadata from Hive.
  *
  * TODO- rewrite this using HiveMetaStoreClient for much better performance.
  */
class GetMetadataAction(conf : Map[String, String], node : Node, throttle: Boolean = true) {

  private val logger = LoggerFactory.getLogger(classOf[GetMetadataAction])

  def apply(dbTablePartSpec: String) : TableInfo = {
    //parse the configured source tables of form "$db.$table (part1=$part1, part2=$part2) (part1=$part1, part2=$part3)"
    val regex = """(\S*)\.(\S*)\s*(.*)""".r

    dbTablePartSpec match {
      case regex(db, table, partSpec) => {
        //1. Get the table metadata, like location, isPartitioned, and createStmt.
        val getTableMetadataAction = new GetTableMetadataAction(conf, node)
        val createTable = getTableMetadataAction(db, table)

        //2.  If partitioned, get the list of partitions.
        val partitionList: Array[String] =
          if (createTable.partitionedBy.length != 0) {
            if (partSpec.isEmpty) {
              ListPartitionAction(conf, node, db, table, None, throttle)
            } else {
              val parenRegex = """\((.*?)\)""".r
              parenRegex.findAllIn(partSpec).flatMap(
                p => ListPartitionAction(conf, node, db, table, Some(p), throttle)).toArray.distinct
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
        val partitions = getPartitionAction(db, table, createTable, partitionSpecList)
        TableInfo(db, createTable, partitions)
      }
      case _ => throw new IllegalArgumentException(s"Cannot parse ${CopyConstants.sourceTables}: $dbTablePartSpec.  " +
        "Make sure it is of form $db.$table $partition, where $partition is optional and of form (part1='val1', part2='val2').")
    }
  }
}
