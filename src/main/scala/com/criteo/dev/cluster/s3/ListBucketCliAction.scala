package com.criteo.dev.cluster.s3

import com.criteo.dev.cluster.{CliAction, Public}
import com.criteo.dev.cluster.aws.AwsUtilities

import scala.collection.JavaConverters._

/**
  * Lists all buckets.
  */
@Public object ListBucketCliAction extends CliAction[Set[BucketMeta]] {
  override def command: String = "list-bucket"

  override def usageArgs: List[Any] = List()

  override def help: String = "List all buckets"

  override def applyInternal(args: List[String], conf: Map[String, String]): Set[BucketMeta] = {
    val s3Client = BucketUtilities.getS3Client(conf)
    val buckets = s3Client.listOwnedBuckets().asScala

    //Not sure how to use Jclouds to do tagging.  For now, unfortunately just use name prefix.
    val devBuckets = buckets.filter(u => u.getName().startsWith(BucketUtilities.getS3BucketPrefix(conf)))
    devBuckets.foreach(u => println(u.getName()))

    //return modeled buckets
    devBuckets.map(db => new BucketMeta(db.getName)).toSet
  }
}
