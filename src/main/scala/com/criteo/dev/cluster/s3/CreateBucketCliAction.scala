package com.criteo.dev.cluster.s3

import com.criteo.dev.cluster.{CliAction, Public}
import com.criteo.dev.cluster.aws.{AwsConstants, AwsUtilities}
import com.criteo.dev.cluster.config.GlobalConfig
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

  override def applyInternal(args: List[String], config: GlobalConfig): BucketMeta = {

    val conf = config.backCompat
    //generate bucket name
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
    BucketUtilities.createBucket(conf, bucketName)
  }
}
