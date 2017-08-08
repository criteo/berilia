package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.command.SshAction
import com.criteo.dev.cluster.{Node, NodeFactory}
import com.criteo.dev.cluster.s3._

import scala.util.Random

/**
  * Copies via a temporary bucket.  Uses distcp twice, first from source cluster to temp bucket, then from temp bucket to target.
  *
  * This is to get around security firewall restrictions between source and target clusters.
  */
class CopyViaBucketAction (conf: Map[String, String], source: Node, target: Node)
  extends CopyFileAction (conf, source, target) {

  override def apply(sourceFiles: Array[String], sourceBase: String, targetBase: String): Unit = {

    //create temp bucket
    val tempBucket = createTempBucket
    val tempBucketNode = NodeFactory.getS3Node(tempBucket.name)

    //first copy
    new DistCpS3Action(conf, source, tempBucketNode).apply(sourceFiles, sourceBase, targetBase)

    //second copy
    val folders = BucketUtilities.getAllFolders(conf, tempBucket.name, target.nodeType)
    folders.foreach(f => SshAction(target, s"hadoop distcp $f /"))

    //delete temp bucket
    DestroyBucketCliAction.destroy(conf, tempBucket.name)
  }

  def createTempBucket : BucketMeta = {
    val awsBucketPrefix = BucketUtilities.getS3BucketPrefix(conf)
    val random = new Random()
    val randomInt = random.nextInt(10000)
    val bucketPrefix = s"$awsBucketPrefix-${BucketConstants.tempBucketPrefix}-${randomInt}"
    BucketUtilities.createBucket(conf, bucketPrefix)
  }
}
