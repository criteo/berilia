package com.criteo.dev.cluster.s3

import com.criteo.dev.cluster.aws.AwsUtilities
import com.criteo.dev.cluster.{NodeFactory, SshHiveAction}
import org.slf4j.LoggerFactory

/**
  * Attaches a given cluster to a S3 bucket, by running ddl on that cluster to create tables
  * pointing to data stored in the bucket.
  */
object AttachDdlAction {

  private val logger = LoggerFactory.getLogger(AttachDdlAction.getClass)

  def apply(bucketId: String, conf: Map[String, String]) = {
    val blobStore = BucketUtilities.getBlobStore(conf)
    val fileList = BucketUtilities.getSortedLogs(blobStore, bucketId)
    val ddlList = fileList.filter(f => {
      BucketUtilities.getDataType(f).equals(DataType.hive)
    })
    val ddl = new SshHiveAction(NodeFactory.getTarget(conf))

    ddlList.foreach(d => {
      val blob = blobStore.getBlob(bucketId, d.getName())
      val payload = blob.getPayload()
      val content = BucketUtilities.getContent(payload)

      logger.info("Running following DDL")
      println
      content.foreach(c => println(c))
      content.foreach(c => ddl.add(c))
    })
    ddl.run
    logger.info("Successfully created metadata on cluster pointing to S3 bucket")
  }
}
