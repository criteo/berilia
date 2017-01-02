package com.criteo.dev.cluster.s3

import com.criteo.dev.cluster.{CliAction, Public}
import com.criteo.dev.cluster.aws.AwsUtilities
import org.slf4j.LoggerFactory

/**
  * Destroy S3 bucket.
  */
@Public object DestroyBucketCliAction extends CliAction[Unit] {

  private val logger = LoggerFactory.getLogger(CreateBucketCliAction.getClass)

  override def command: String = "destroy-bucket"

  override def usageArgs: List[Any] = List("Bucket-id")

  override def help: String = "Deletes a bucket in S3"

  override def applyInternal(args: List[String], conf: Map[String, String]): Unit = {
    val bucketId = args(0)
    destroy(conf, bucketId)
  }

  def destroy(conf: Map[String, String], bucketId: String) = {
    logger.info(s"Deleting bucket $bucketId")

    require(bucketId.startsWith(BucketUtilities.getS3BucketPrefix(conf)), "Only allowed to delete buckets created by dev-cluster program.")

    val blobStore = BucketUtilities.getBlobStore(conf)
    blobStore.deleteContainer(bucketId)
    logger.info(s"Deleted bucket $bucketId")
  }
}
