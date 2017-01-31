package com.criteo.dev.cluster.aws

import com.criteo.dev.cluster._

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

    val mountDirs = getDataDirs(NodeFactory.getAwsNode(conf, cluster.master))
    CopyConfAction(conf, cluster, mountDirs)

    AwsUtilities.printClusterInfo(conf, cluster)
  }

  def getDataDirs(node: Node) : List[String] = {
    val blockInfo = SshAction(node, "lsblk", returnResult = true).stripLineEnd
    val blockLines = blockInfo.split("\n")
    require (blockLines(0).split("\\s+")(6).equalsIgnoreCase("mountpoint"),
      s"Mount point not in expected position in lsblk output: ${blockLines(0)}")
    val mountedBlockLines = blockLines.filter(_.split("\\s+").length > 6)
    val mountLocations = mountedBlockLines.map(_.split("\\s+")(6))
    mountLocations.filter(_.startsWith("/" + GeneralConstants.data)).toList
  }
}
