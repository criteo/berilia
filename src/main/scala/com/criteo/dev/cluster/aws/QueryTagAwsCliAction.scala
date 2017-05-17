package com.criteo.dev.cluster.aws

import com.criteo.dev.cluster.CliAction
import com.criteo.dev.cluster.config.GlobalConfig

/**
  * Query AWS clusters for a certain tag.
  */
object QueryTagAwsCliAction extends CliAction[List[AwsCluster]] {
  override def command: String = "query-tag-aws"

  override def usageArgs: List[Any] = List("tag.name", "tag.value")

  override def help: String = "Query AWS clusters for a certain tag."

  override def applyInternal(args: List[String], config: GlobalConfig): List[AwsCluster] = {
    val tagName = args(0)
    val tagValue = args(1)
    val conf = config.backCompat

    val clusters = AwsUtilities.getAllClusters(conf)
    val matches = clusters.filter(c => {
      val clusterMeta = c.master.getUserMetadata
      val key = clusterMeta.get(AwsConstants.userTagPrefix + tagName)
      key != null && key.equals(tagValue)
    })

    matches.foreach(m => AwsUtilities.printClusterInfo(conf, m))
    matches.map(c => AwsUtilities.getAwsCluster(conf, c)).toList
  }
}
