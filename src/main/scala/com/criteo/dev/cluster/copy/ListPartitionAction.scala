package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster._
import com.criteo.dev.cluster.docker.DockerCopyFileAction
import org.slf4j.LoggerFactory

class ListPartitionAction(conf: Map[String, String], node: Node, throttle: Boolean = true) {

  private val logger = LoggerFactory.getLogger(ListPartitionAction.getClass)

  def apply(database: String, table: String, partSpec: Option[String]): Array[String] = {
        val sshHiveAction = new SshHiveAction(node, ignoreError=false)
    sshHiveAction.add(s"use $database")

    val hiveQuery = new StringBuilder(s"show partitions $table")
    if (partSpec.isDefined) {
      hiveQuery.append(s" partition ${partSpec.get}")
    }
    sshHiveAction.add(hiveQuery.toString)
    val hiveResult = sshHiveAction.run
    val resultList: Array[String] = if (!hiveResult.isEmpty) {
      hiveResult.split("\n")
    } else {
      Array()
    }

    if (partSpec.isDefined && resultList.length == 0) {
      throw new IllegalArgumentException(s"partspec returns no result: ${partSpec.get}")
    }

    //Throttle number of partitions.
    val result = getAbsoluteParts(database, table, resultList)

    logger.info(s"Selecting following ${result.length} partitions from $database.$table:")
    result.foreach(u => println(u))
    result
  }



  def getAbsoluteParts(database: String, table: String, resultList: Array[String]) : Array[String] = {
    val limit = {
      val limitConf = CopyUtilities.getOverridableConf(conf, database, table, CopyConstants.absolutePartCount)
      Math.min(limitConf.toInt, resultList.length)
    }
    // -1 configured limit, means no limit
    if (throttle && limit > 0) {
      resultList.takeRight(limit)
    } else {
      resultList
    }
  }

}

object ListPartitionAction {
  def apply(conf: Map[String, String],
            node: Node,
            database: String,
            table: String,
            partSpec: Option[String],
            throttle: Boolean=true) : Array[String] = {
    val action = new ListPartitionAction(conf, node, throttle)
    action.apply(database, table, partSpec)
  }
}
