package com.criteo.dev.cluster.aws

import com.criteo.dev.cluster.command.SshAction
import com.criteo.dev.cluster.{command, _}
import com.criteo.dev.cluster.config.AWSConfig
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Mounts unmounted disks.
  */
object ConfigureDiskAction {

  private val logger = LoggerFactory.getLogger(ConfigureDiskAction.getClass)

  def apply(conf: AWSConfig, cluster: JcloudCluster) : List[String] = {

    logger.info(s"Mounting disks on ${cluster.size} host(s) in parallel")

    val nodes = cluster.slaves + cluster.master
    val allFutures = nodes.map(n => GeneralUtilities.getFuture {
      configureDisk(NodeFactory.getAwsNode(conf, n))
    })
    val aggFuture = Future.sequence(allFutures)
    val result: mutable.Set[List[String]] = Await.result(aggFuture, Duration.Inf)
    result.toIterator.next()
  }

  /**
    * Lists all unmounted partitions and mounts them.
    */
  def configureDisk(node: Node) : List[String] = {
    val result = SshAction(node, "lsblk", returnResult = true).stripLineEnd
    logger.info(s"Block information on ${node.ip}:")
    val lines = result.split("\n").map(_.trim)
    require(lines(0).trim.split("\\s+")(6).equalsIgnoreCase("MOUNTPOINT"),
      s"Mount point not in expected position in lsblk output: ${lines(0)}")

    //this is a bit delicate, but assuming the unmounted ones are at the end,
    //then we will take the ones up to the first one that has a mount entry.
    val toMount = lines.reverse.takeWhile(l => l.split("\\s+").length <= 6).map(l => l.split("\\s+")(0))
    val mountCommands = toMount.zipWithIndex.flatMap { case (tm, i) =>
      List(
        s"sudo echo -e 'o\\nn\\np\\n1\\n\\n\\nw' | sudo fdisk /dev/$tm", // create one partition (n, p, 1, default start, default end of sector)
        s"sudo /sbin/mkfs.ext4 /dev/${tm}1", // make fs
        s"sudo mkdir -p /${GeneralConstants.data}/$i",
        s"sudo mount /dev/${tm}1 /${GeneralConstants.data}/$i" // mount
      )
    }.toList
    command.SshMultiAction(node, mountCommands)

    0.to(toMount.length - 1).map(i => s"/${GeneralConstants.data}/$i").toList
  }
}
