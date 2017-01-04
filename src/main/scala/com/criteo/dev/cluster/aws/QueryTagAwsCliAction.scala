package com.criteo.dev.cluster.aws

import com.criteo.dev.cluster.CliAction

/**
  * Query AWS clusters for a certain tag.
  */
object QueryTagAwsCliAction extends CliAction[Unit] {
  override def command: String = "query-tag-aws"

  override def usageArgs: List[Any] = List("tag.name", "tag.value")

  override def help: String = "Query AWS clusters for a certain tag."

  override def applyInternal(args: List[String], conf: Map[String, String]): Unit = {
    val tagName = args(0)
    val tagValue = args(1)

    val clusters = AwsUtilities.getAllClusters(conf)
    val matches = clusters.filter(c => {
      val clusterMeta = c.master.getUserMetadata
      val key = clusterMeta.get(AwsConstants.userTagPrefix + tagName)
      key != null && key.equals(tagValue)
    })

    matches.foreach(m => AwsUtilities.printClusterInfo(conf, m))
  }
}
