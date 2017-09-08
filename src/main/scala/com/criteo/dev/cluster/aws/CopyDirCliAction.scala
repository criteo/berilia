package com.criteo.dev.cluster.aws

import com.criteo.dev.cluster._
import com.criteo.dev.cluster.command.{RsyncAction, SshMultiAction}
import com.criteo.dev.cluster.config.GlobalConfig

/**
  * Copy directory from local to AWS cluster (ex, for user libraries)
  */
@Public object CopyDirCliAction extends CliAction[Unit] {
  override def command: String = "copy-dir"

  override def usageArgs: List[Any] = List("cluster.id", "copy.dir")

  override def help: String = "Copies a local directory to AWS cluster"

  override def applyInternal(args: List[String], config: GlobalConfig): Unit = {
    val instanceId = args(0)
    val conf = config.backCompat
    val cluster = AwsUtilities.getUserCluster(conf, instanceId)
    val toCopy = args(1)
    copyDir(NodeFactory.getAwsNode(config.app.aws, cluster.master), toCopy)
  }

  def copyDir(node: Node, dir: String) = {
    val sshMultiAction = new SshMultiAction(node)
    sshMultiAction.add(s"sudo mkdir -p ${AwsConstants.copyLocation}")
    sshMultiAction.add(s"sudo chmod 777 ${AwsConstants.copyLocation}")
    sshMultiAction.run()
    RsyncAction(dir, node, AwsConstants.copyLocation)
  }
}
