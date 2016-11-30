package com.criteo.dev.cluster.s3

import com.criteo.dev.cluster.aws.AwsUtilities
import com.criteo.dev.cluster.copy.CopyUtilities
import com.criteo.dev.cluster.{Node, NodeFactory, SshHiveAction}
import org.slf4j.LoggerFactory

/**
  * Attaches a given cluster to a S3 bucket, by running ddl on that cluster to create tables
  * pointing to data stored in the bucket.
  */
object RunS3DdlAction {

  private val logger = LoggerFactory.getLogger(RunS3DdlAction.getClass)

  def apply(node: Node, bucketId: String, copiedLocally: Boolean, conf: Map[String, String]) = {
    val blobStore = BucketUtilities.getBlobStore(conf)
    val fileList = BucketUtilities.getSortedLogs(blobStore, bucketId)
    val ddlList = fileList.filter(f => {
      BucketUtilities.getDataType(f).equals(DataType.hive)
    })
    val ddl = new SshHiveAction(node)

    ddlList.foreach(d => {
      val blob = blobStore.getBlob(bucketId, d.getName())
      val payload = blob.getPayload()
      val content = BucketUtilities.getContent(payload)

      logger.info("Running following DDL")
      println
      content.map(c =>
        if (copiedLocally) {
          c.replace("$LOCATION", "")
        } else {
          c.replace("$LOCATION", BucketUtilities.getS3Location(conf, bucketId, node.nodeType,
            includeCredentials=true))
      }).foreach(c => {
          println(c)
          ddl.add(c)
      })
    })
    ddl.run
    logger.info("Successfully created metadata on cluster for data on the S3 bucket")
  }
}
