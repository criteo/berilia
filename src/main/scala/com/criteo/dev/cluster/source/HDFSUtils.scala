package com.criteo.dev.cluster.source

import com.criteo.dev.cluster.{Node, SshAction}

object HDFSUtils {

  /**
    * Get the list of file sizes, given a list of HDFS file locations
    *
    * @param locations List of file locations
    * @param node      The node
    * @return List of file sizes
    */
  def getFileSize(locations: List[String], node: Node): List[Long] =
    SshAction
      .apply(node, s"hdfs dfs -du -s ${locations.mkString(" ")}", true)
      .split("\n")
      .map(_.split(" ").head.toLong)
      .toList
}
