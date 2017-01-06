package com.criteo.dev.cluster.aws

import com.criteo.dev.cluster.CliAction

/**
  * List-all AWS instances
  */
object ListAllAwsCliAction extends CliAction [List[AwsCluster]] {
  override def command: String = "list-all-aws"

  override def usageArgs: List[Any] = List()

  override def help: String = "Lists all clusters owned by all users, and details of nodes within the clusters."

  override def applyInternal(args: List[String], conf: Map[String, String]): List[AwsCluster] = {
    val results = AwsUtilities.getAllClusters(conf)
    results.foreach(AwsUtilities.printClusterInfo(conf, _, includeOwner = true))
    results.map(c => AwsUtilities.getAwsCluster(conf, c)).toList
  }
}
