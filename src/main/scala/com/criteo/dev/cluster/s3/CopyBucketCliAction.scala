package com.criteo.dev.cluster.s3

import com.criteo.dev.cluster.aws.{AwsConstants, CopyAwsCliAction}
import com.criteo.dev.cluster.copy._
import com.criteo.dev.cluster.{CliAction, GeneralConstants, NodeFactory, Public}
import org.slf4j.LoggerFactory

/**
  * Copies data from configured source to the bucket.
  */
@Public
object CopyBucketCliAction extends CliAction[Unit] {

  private val logger = LoggerFactory.getLogger(CopyAwsCliAction.getClass)

  override def command: String = "copy-bucket"

  override def usageArgs: List[Any] = List("bucket-id")

  override def help: String = "Copies data from configured source to a bucket."

  override def applyInternal(args: List[String], conf: Map[String, String]): Unit = {
    //find the ip
    val bucketId = args(0)
    val s3Client = BucketUtilities.getS3Client(conf)
    require(s3Client.bucketExists(bucketId), s"Bucket $bucketId not found.")
    val target = NodeFactory.getS3Node(conf, bucketId)
    val source = NodeFactory.getSourceFromConf(conf)

    //parse source-tables.
    CopyAllAction(conf, source, target)
  }
}
