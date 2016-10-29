package com.criteo.dev.cluster.s3

import com.criteo.dev.cluster.{CliAction, Public}
import com.criteo.dev.cluster.aws.{AwsConstants, AwsUtilities}
import org.jclouds.s3.domain.CannedAccessPolicy
import org.jclouds.s3.options.PutBucketOptions
import org.slf4j.LoggerFactory

import scala.util.Random

/**
  * Create S3 bucket
  */
@Public object CreateBucketCliAction extends CliAction[BucketMeta] {

  private val logger = LoggerFactory.getLogger(CreateBucketCliAction.getClass)

  override def command: String = "create-bucket"

  override def usageArgs: List[Any] = List(Option("Name"))

  override def help: String = "Creates a bucket in S3"

  override def applyInternal(args: List[String], conf: Map[String, String]): BucketMeta = {

    val s3Client = BucketUtilities.getS3Client(conf)
    val awsBucketPrefix = BucketUtilities.getS3BucketPrefix(conf)

    val bucketName = {
      if (args.length == 1) {
        val name = args(0)
        s"$awsBucketPrefix-$name"
      } else {
        val random = new Random()
        val randomInt = random.nextInt(10000)
        s"$awsBucketPrefix-${randomInt}"
      }

    }
    val region = AwsUtilities.getAwsProp(conf, AwsConstants.region)

    var putBucketOptions = new PutBucketOptions
    putBucketOptions.setHeaderTag(AwsConstants.groupTag)
    putBucketOptions = putBucketOptions.withBucketAcl(CannedAccessPolicy.PRIVATE)
    s3Client.putBucketInRegion(region, bucketName, putBucketOptions)

    logger.info(s"Created bucket $bucketName")
    BucketMeta(bucketName)
  }
}
