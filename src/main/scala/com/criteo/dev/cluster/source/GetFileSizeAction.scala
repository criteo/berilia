package com.criteo.dev.cluster.source

import com.criteo.dev.cluster.{Node, SshAction}

case class GetFileSizeAction(
                               conf: Map[String, String],
                               node: Node
                             ) {
  /**
    * Get the list of file sizes, given a list of HDFS file locations
    *
    * @param locations List of file locations
    * @return List of file sizes
    */
  def apply(locations: List[String]): List[Long] = {
    SshAction
      .apply(node, s"hdfs dfs -du -s ${locations.mkString(" ")}", true)
      .split("\n")
      .map(_.split(" ").head.toLong)
      .toList
  }
}
