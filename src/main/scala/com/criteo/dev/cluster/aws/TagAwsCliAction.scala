package com.criteo.dev.cluster.aws

import com.criteo.dev.cluster.CliAction
import com.criteo.dev.cluster.config.GlobalConfig

/**
  * Tag AWS cluster.
  */
object TagAwsCliAction extends CliAction[Unit] {
  override def command: String = "tag-aws"

  override def usageArgs: List[Any] = List("instance.id", "tag.name", "tag.value")

  override def help: String = "Tag a particular AWS cluster with given tag.name and tag.value."

  override def applyInternal(args: List[String], config: GlobalConfig): Unit = {
    val instanceId = args(0)
    val tagName = args(1)
    val tagValue = args(2)
    val conf = config.backCompat

    val cluster = AwsUtilities.getUserCluster(conf, instanceId)
    AwsUtilities.setTag(conf, cluster, AwsConstants.userTagPrefix + tagName, Some(tagValue))
  }
}
