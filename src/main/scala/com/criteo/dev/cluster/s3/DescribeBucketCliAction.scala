package com.criteo.dev.cluster.s3

import com.criteo.dev.cluster.{CliAction, Public}
import com.criteo.dev.cluster.aws.AwsUtilities
import org.slf4j.LoggerFactory

/**
  * Command to let users see what kind of Hive metadata and HDFS files are stored in S3.
  */
@Public object DescribeBucketCliAction extends CliAction[BucketDataMeta] {
  override def command: String = "describe-bucket"

  override def usageArgs: List[Any] = List("bucket-id")

  override def help: String = "Shows contents of bucket."

  private val logger = LoggerFactory.getLogger(CreateBucketCliAction.getClass)

  override def applyInternal(args: List[String], conf: Map[String, String]): BucketDataMeta = {
    val bucketId = args(0)
    logger.info(s"Describing bucket $bucketId")

    require(bucketId.startsWith(BucketUtilities.getS3BucketPrefix(conf)), "Only allowed to access buckets created by dev-cluster program.")

    val blobStore = BucketUtilities.getBlobStore(conf)
    val fileList = BucketUtilities.getSortedLogs(blobStore, bucketId)


    val results = fileList.map(f => {
      BucketUtilities.getBucketDataSet(f, blobStore, bucketId)
    })

    results.foreach(r => BucketUtilities.printBucketDataSet(r))
    BucketDataMeta(bucketId, results)
  }
}