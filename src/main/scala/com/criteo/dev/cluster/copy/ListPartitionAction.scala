package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster._
import com.criteo.dev.cluster.docker.DockerCopyFileAction
import org.slf4j.LoggerFactory

class ListPartitionAction(conf: Map[String, String], node: Node, throttle: Boolean = true) {

  private val logger = LoggerFactory.getLogger(ListPartitionAction.getClass)

  def apply(database: String, table: String): Array[String] = {

    val queryResult = SshAction(node,
      "hive --database " + database + " -e 'show partitions " + table + ";'", returnResult = true)
    val resultList = queryResult.split("\n")

    //Throttle number of partitions.
    val result = getAbsoluteParts(database, table, resultList)

    logger.info(s"Will sample last ${result.length} partitions from $database.$table:")
    result.foreach(u => println(u))
    result
  }



  def getAbsoluteParts(database: String, table: String, resultList: Array[String]) : Array[String] = {
    val limit = {
      val limitConf = CopyUtilities.getOverridableConf(conf, database, table, CopyConstants.absolutePartCount)
      Integer.min(limitConf.toInt, resultList.length)
    }
    if (throttle) {
      resultList.takeRight(limit)
    } else {
      resultList
    }
  }

}

object ListPartitionAction {
  def apply(conf: Map[String, String], node: Node, database: String, table: String, throttle: Boolean=true) : Array[String] = {
    val action = new ListPartitionAction(conf, node, throttle)
    action.apply(database, table)
  }
}