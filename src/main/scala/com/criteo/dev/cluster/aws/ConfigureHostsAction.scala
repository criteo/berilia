package com.criteo.dev.cluster.aws

import com.criteo.dev.cluster.config.AWSConfig
import com.criteo.dev.cluster.{GeneralConstants, GeneralUtilities, NodeFactory, SshMultiAction}
import org.jclouds.compute.domain.NodeMetadata
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Setup a pseudo-network between the clusters by modifying /etc/hosts file, creating an alias of private IP with
  * short host name.
  *
  * We use in hadoop configuration these short host names.
  */
object ConfigureHostsAction {

  private val logger = LoggerFactory.getLogger(ConfigureHostsAction.getClass)

  def apply(conf: AWSConfig, newClusters: Iterable[JcloudCluster]) = {
    val reconfigFutures = newClusters.map(c => GeneralUtilities.getFuture {
      fixHosts(conf, c)
    })

    val aggReconfigFuture = Future.sequence(reconfigFutures)
    Await.result(aggReconfigFuture, Duration.Inf)
  }


  /**
    * Setup a pseudo-network to allow nodes of a cluster to know about each other.
    */
  def fixHosts(conf: AWSConfig, cluster: JcloudCluster) = {

    logger.info(s"Configuring ${cluster.size} host(s) in parallel")

    //configure master
    val configureMasterFuture = GeneralUtilities.getFuture {
      editEtcHosts(conf, cluster.master, cluster)
      ""  //return something, otherwise future throws some exception
    }

    //configure the slaves.
    val allFutures = List(configureMasterFuture) ++ cluster.slaves.map {sm =>
      GeneralUtilities.getFuture {
        editEtcHosts(conf, sm, cluster)
        "" //return something, otherwise future throws some exception
      }
    }

    val aggFutures = Future.sequence(allFutures)
    Await.result(aggFutures, Duration.Inf)
  }

  /**
    * Important, we need to use internal-ips and not the external-ips.  The nodes among the cluster only know each
    * other via the internal ip, and cannot work with each other through external ones
 *
    * @param conf
    * @param target node to run action on
    * @param cluster cluster metadata (will enter data about the other nodes)
    */
  def editEtcHosts(conf: AWSConfig, target: NodeMetadata, cluster: JcloudCluster) : Unit = {
    val node = NodeFactory.getAwsNode(conf, target)
    val sshAction = new SshMultiAction(node)
    sshAction.add("echo \"127.0.0.1   localhost localhost.localdomain\" | sudo tee --append /etc/hosts")
    sshAction.add("echo \"127.0.0.1 $(hostname)\" | sudo tee --append /etc/hosts")
    sshAction.add("echo \"" + AwsUtilities.privateIp(cluster.master) + " " +
      GeneralConstants.masterHostName + "\" | sudo tee --append /etc/hosts")
    cluster.slaves.foreach(sm => {

      val slaveName = sm.getUserMetadata.get(AwsConstants.hostName)
      val ip = AwsUtilities.privateIp(sm)
      sshAction.add("echo \"" + s"${ip} $slaveName" + "\" | sudo tee --append /etc/hosts")
    })
    sshAction.run()
  }
}
