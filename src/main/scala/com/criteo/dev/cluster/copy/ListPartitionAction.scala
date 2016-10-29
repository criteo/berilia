package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster._
import com.criteo.dev.cluster.docker.DockerCopyFileAction
import org.slf4j.LoggerFactory

class ListPartitionAction(conf: Map[String, String], node: Node) {

  private val logger = LoggerFactory.getLogger(ListPartitionAction.getClass)

  def apply(database: String, table: String): Array[String] = {

    val queryResult = SshAction(node,
      "hive --database " + database + " -e 'show partitions " + table + ";'", returnResult = true)
    val resultList = queryResult.split("\n")

    //Throttle number of partitions.
    val result = getPartLimit(database, table, resultList)

    logger.info(s"Will sample last ${result.length} partitions from $database.$table:")
    result.foreach(u => println(u))
    result
  }


  def getPartLimit (database: String, table: String, resultList: Array[String]) : Array[String] = {
    def targetType = GeneralUtilities.getConfStrict(conf, GeneralConstants.targetTypeProp, "Internally-provided")
    targetType match {
      case (GeneralConstants.awsType) => getAbsoluteParts(database, table, resultList)
      case (GeneralConstants.localClusterType) => getAbsoluteParts(database, table, resultList)
      case (GeneralConstants.s3Type) => getTopParts(database, table, resultList)
      case _ => throw new IllegalArgumentException(s"Unsupported target type: $targetType")
    }
  }

  def getTopParts(database: String, table: String, resultList: Array[String]) : Array[String] = {
    val topPartLimit = getPartLimitConf(database, table, CopyConstants.topPartCount)
    val partitionSpecList: Array[PartSpec] = resultList.map(s => {
      CopyUtilities.getPartInfos(s)
    })

    val topPartialPartitions = partitionSpecList.map(u => u.specs(0)).distinct
    val limit = Integer.min(topPartLimit, topPartialPartitions.length)
    val limitedTopPartialPartitions = topPartialPartitions.takeRight(limit).toSet
    resultList.filter(s => {
      limitedTopPartialPartitions.contains(CopyUtilities.getPartInfos(s).specs(0))
    })
  }

  def getAbsoluteParts(database: String, table: String, resultList: Array[String]) : Array[String] = {
    val limit = {
      val limitConf = getPartLimitConf(database, table, CopyConstants.absolutePartCount)
      Integer.min(limitConf, resultList.length)
    }
    resultList.takeRight(limit)
  }

  def getPartLimitConf(database: String, table: String, prop: String) : Integer = {
    val limitConf = conf.get(s"$database.$table.$prop")
    val limit = {
      if (limitConf.isEmpty) {
        GeneralUtilities.getConfStrict(conf, s"default.$prop", GeneralConstants.sourceProps)
      } else {
        limitConf.get
      }
    }
    Integer.valueOf(limit)
  }

}

object ListPartitionAction {
  def apply(conf: Map[String, String], node: Node, database: String, table: String) : Array[String] = {
    val action = new ListPartitionAction(conf, node)
    action.apply(database, table)
  }
}