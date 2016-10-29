package com.criteo.dev.cluster.s3

import com.criteo.dev.cluster.Node
import com.criteo.dev.cluster.aws.{AwsConstants}
import com.criteo.dev.cluster.s3.DataType.DataType
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
  * Uploads a file to S3
  */
object UploadS3Action {

  private val logger = LoggerFactory.getLogger(UploadS3Action.getClass)

  def apply(conf: Map[String, String], bucket: Node, dataType: DataType, contents: List[String]) = {
    val bucketId = bucket.ip

    val content = contents.mkString("\n")
    logger.info(content)
    val payload = BucketUtilities.getByteSource(content)

    val path = BucketUtilities.getBucketDateTime

    val blobStore = BucketUtilities.getBlobStore(conf)
    val metadata = mapAsJavaMap(Map(AwsConstants.userTagKey -> System.getenv("USER"),
      BucketConstants.dataType -> dataType.toString))
    val blob = blobStore.blobBuilder(path)
      .payload(payload)
      .contentLength(payload.size())
      .userMetadata(metadata)
      .build();
    blobStore.putBlob(bucketId, blob);

    logger.info(s"Uploaded metadata into bucket $bucketId under path $path")
  }

}
