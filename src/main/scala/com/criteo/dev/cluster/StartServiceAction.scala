package com.criteo.dev.cluster

import com.criteo.dev.cluster.aws.AwsUtilities
import com.criteo.dev.cluster.aws.AwsUtilities.NodeRole
import com.criteo.dev.cluster.aws.AwsUtilities.NodeRole.NodeRole
import com.criteo.dev.cluster.command.{ScpAction, SshAction}
import org.slf4j.LoggerFactory

/**
 * Start hadoop and other services on a given node.
 */
object StartServiceAction {

  private val logger = LoggerFactory.getLogger(StartServiceAction.getClass)

  def apply(node: Node, nodeRole: NodeRole): Unit = {
    val startScript = AwsUtilities.getStartServiceScript(nodeRole)
    logger.info(s"Running $startScript on node ${node.ip}")
    ScpAction(
      sourceN = None,
      srcPath = s"${GeneralConstants.setupScriptDir}/$startScript",
      targetN = Some(node),
      targetPath = "~/start.sh")
    SshAction(node, s"source start.sh")
    SshAction(node, "rm start.sh")
  }
}
