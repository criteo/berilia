package com.criteo.dev.cluster.aws

import com.criteo.dev.cluster.{CliAction, Public}

/**
  * Copies the hadoop-configuration under /hadoop-conf/${target.hadoop.conf.dir} to a cluster
  */
@Public object ConfigureAwsCliAction extends CliAction[Unit] {
  override def command: String = "configure-aws"

  override def usageArgs: List[Any] = List("cluster.id")

  override def help: String = "Copies the hadoop-configuration in /hadoop-resources/hadoop-conf/${target.hadoop.conf.dir}" +
    " to the specified AWS cluster.  Restart-services-aws may be required."

  override def applyInternal(args: List[String], conf: Map[String, String]): Unit = {
    val instanceId = args(0)
    val cluster = AwsUtilities.getUserCluster(conf, instanceId)
    CopyConfAction(conf, cluster)
  }
}
