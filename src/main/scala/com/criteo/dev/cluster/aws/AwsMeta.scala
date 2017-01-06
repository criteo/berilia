package com.criteo.dev.cluster.aws

import com.criteo.dev.cluster.Public

/**
  * User-facing class to model AWS nodes and clusters.
  */
@Public case class AwsNodeMeta (id: String,
                                publicIp: String,
                                privateIp: String,
                                status: AwsNodeState,
                                shortHostName: String)

@Public case class AwsCluster (master: AwsNodeMeta,
                               slaves: Set[AwsNodeMeta],
                               createTime: String,
                               expireTime: String,
                               tags: Map[String, String],
                               user: String)


//Represents state of an AWS Node
@Public sealed abstract class AwsNodeState(name: String) {
  override def toString = name
}

@Public object AwsPending extends AwsNodeState("Pending")
@Public object AwsTerminated extends AwsNodeState("Terminated")
@Public object AwsSuspended extends AwsNodeState("Suspended")
@Public object AwsRunning extends AwsNodeState("Running")
@Public object AwsError extends AwsNodeState("Error")

@Public class OtherAwsState(name: String) extends AwsNodeState(name)