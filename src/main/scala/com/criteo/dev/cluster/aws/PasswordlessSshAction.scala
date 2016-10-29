package com.criteo.dev.cluster.aws

import com.criteo.dev.cluster.{NodeFactory, SshAction}
import org.slf4j.LoggerFactory

/**
  * Set up passwordless SSH among the nodes, by modifying ./.ssh/authorized_keys on the slaves to include the master.
  */
object PasswordlessSshAction {

  private val logger = LoggerFactory.getLogger(PurgeAwsCliAction.getClass)

  def apply(conf: Map[String, String], cluster: JcloudCluster) : Unit = {
    if (cluster.slaves.size > 0) {
      logger.info("Setting up passwordless ssh among all the nodes")
      val master = NodeFactory.getAwsNode(conf, cluster.master)
      SshAction(master, "ssh-keygen -f ./.ssh/id_rsa -t rsa -N ''")
      val masterPubKey = SshAction(master, "cat ./.ssh/id_rsa.pub", returnResult = true).stripLineEnd
      cluster.slaves.foreach(sm => {
        val slave = NodeFactory.getAwsNode(conf, sm)
        SshAction(slave, "echo \"" + masterPubKey + "\" >> ./.ssh/authorized_keys")
      })
    }
  }
}
