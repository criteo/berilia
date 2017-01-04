package com.criteo.dev.cluster.aws

import com.criteo.dev.cluster.CliAction

/**
  * Untag an AWS cluster.
  */
object UntagAwsCliAction extends CliAction[Unit] {
  override def command: String = "untag-aws"

  override def usageArgs: List[Any] = List("instance.id", "tag.name")

  override def help: String = "Removes the given tag from the given AWS cluster."

  override def applyInternal(args: List[String], conf: Map[String, String]): Unit = {
    val instanceId = args(0)
    val tagName = args(1)

    val cluster = AwsUtilities.getUserCluster(conf, instanceId)
    AwsUtilities.setTag(conf, cluster, AwsConstants.userTagPrefix + tagName, None)
  }
}
