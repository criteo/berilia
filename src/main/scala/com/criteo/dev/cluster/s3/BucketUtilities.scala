package com.criteo.dev.cluster.s3

import com.criteo.dev.cluster.NodeType._
import com.criteo.dev.cluster.{GeneralConstants, GeneralUtilities, NodeType}
import com.criteo.dev.cluster.aws.{AwsConstants, AwsUtilities, ImageGroup}
import com.criteo.dev.cluster.copy.{CopyConstants, CopyUtilities}
import com.criteo.dev.cluster.s3.DataType.DataType
import com.google.common.base.Charsets
import com.google.common.io.ByteSource
import org.jclouds.ContextBuilder
import org.jclouds.blobstore.domain.StorageMetadata
import org.jclouds.blobstore.options.ListContainerOptions
import org.jclouds.blobstore.{BlobStore, BlobStoreContext}
import org.jclouds.io.Payload
import org.jclouds.s3.S3Client
import org.joda.time.DateTime
import org.scala_tools.time.Imports._
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.collection.JavaConverters._

/**
  * Utilities used by S3 operations.
  *
  * Look in AwsUtilities for other API's used by S3 operations.
  */
object BucketUtilities {

  private val logger = LoggerFactory.getLogger(BucketUtilities.getClass)

  def getS3Client(conf: Map[String, String]) : S3Client = {
    val blobStoreContext = getBlobStoreContext(conf)
    blobStoreContext.unwrapApi(classOf[S3Client])
  }

  def getBlobStore(conf: Map[String, String]) : BlobStore = {
    val blobStoreContext = getBlobStoreContext(conf)
    blobStoreContext.getBlobStore
  }

  private def getBlobStoreContext(conf: Map[String, String]) : BlobStoreContext = {
    val keyid = AwsUtilities.getAwsProp(conf, AwsConstants.accessId)
    val key = AwsUtilities.getAwsProp(conf, AwsConstants.accessKey)
    val region = AwsUtilities.getAwsProp(conf, AwsConstants.region)

    logger.info(s"Connecting to S3 API")

    //create the node using jcloud
    ContextBuilder
      .newBuilder("aws-s3")
      .credentials(keyid, key)
      //   .endpoint(s"https://ec2.${region}.amazonaws.com")
      .buildView(classOf[BlobStoreContext])
  }

  def getByteSource(content: String) : ByteSource = {
    ByteSource.wrap(content.getBytes(Charsets.UTF_8));
  }

  def getContent(payload: Payload): List[String] = {
    val inputStream = payload.openStream()
    val fileIter = Source.fromInputStream(inputStream, "UTF-8").getLines()
    fileIter.toList
  }

  def getSortedLogs(blobStore: BlobStore, bucketId: String) : List[StorageMetadata] = {
    val files = blobStore.list(bucketId,
      ListContainerOptions.Builder.inDirectory(BucketConstants.bucketLogDir).withDetails())
    if (files.size() == 0) {
      logger.info(s"No data in bucket $bucketId")
    }

    files.asScala.toList.sortWith((x, y) => (x.getName() < y.getName()))
  }

  def getS3BucketPrefix(conf: Map[String, String]) =
    GeneralUtilities.getConfStrict(conf, BucketConstants.bucketPrefix, GeneralConstants.targetAwsProps)


  //-----
  //Bucket timestamp, it's mostly same as all timestamps are stored in UTC.
  //but a little different as it's in a format that's built for sorting.
  //Utility method provided to make it readable.
  //-----

  def getSortableDtNow: String = {
    val dt = DateTime.now(DateTimeZone.UTC)
    DateTimeFormat.forPattern(BucketConstants.bucketTimestampFormat).print(dt)
  }

  def getReadableFromSortableDt(sortableDt: String) : String = {
    val dt = DateTimeFormat.forPattern(BucketConstants.bucketTimestampFormat).withZoneUTC().parseDateTime(sortableDt)
    DateTimeFormat.forPattern(AwsConstants.dateTimeFormat).print(dt)
  }


  def getBucketDateTime: String = {
    val ts = getSortableDtNow
    s"${BucketConstants.bucketLogDir}/$ts"
  }

  private def getReadableBucketTimestampFromFile(fullFileName: String) : String = {
    val name = fullFileName.stripPrefix(s"${BucketConstants.bucketLogDir}/")
    getReadableFromSortableDt(name)
  }

  def printBucketDataSet(bucketDataBlockMeta: BucketDataBlockMeta) = {
    val user = bucketDataBlockMeta.writer
    val time = bucketDataBlockMeta.date
    bucketDataBlockMeta.dataType match {
      case DataType.hdfs => println(s"HDFS files copied by $user at $time (UTC)")
      case DataType.hive => println(s"Hive tables copied by $user at $time (UTC)")
      case _ => println(s"Bucket metadata by $user at time $time is corrupt")
    }
    println()
    bucketDataBlockMeta.filteredListing.foreach(s => println(s))
    println()
  }

  def getBucketDataSet(fileMeta: StorageMetadata, blobStore: BlobStore, bucketId: String) : BucketDataBlockMeta = {
    val user = fileMeta.getUserMetadata.get(AwsConstants.userTagKey)
    val time = getReadableBucketTimestampFromFile(fileMeta.getName())
    val dataType = getDataType(fileMeta)

    val blob = blobStore.getBlob(bucketId, fileMeta.getName())
    val payload = blob.getPayload()
    val content = BucketUtilities.getContent(payload)

    val filteredListing = content.filter(u => !u.toLowerCase.startsWith("use"))
      .filter(u => !u.toLowerCase.startsWith("create database"))
      //readability, no need to print out db info, which is included in the table statement.

      .map(u => {
      val index = u.toLowerCase().indexOf("location")
      index match {
        //readability, no need to print user with the location (which is just the bucket they passed in).
        case -1 => u
        case _ => u.substring(0, index)
      }
    })
    BucketDataBlockMeta(user, filteredListing, time, dataType)
  }

  def getDataType(file: StorageMetadata) : DataType = {
    DataType.withName(file.getUserMetadata.get(BucketConstants.dataType))
  }

  def toS3Location(conf: Map[String, String],
                   bucketName: String,
                   sourceLocation: String,
                   accessNodeType: NodeType,
                   includeCredentials: Boolean) : String = {
    val relativeLocation = CopyUtilities.toRelative(sourceLocation)
    val bucketLocation = getS3Location(conf, bucketName, accessNodeType, includeCredentials)
    bucketLocation + relativeLocation
  }

  def getS3Location(conf: Map[String, String],
                    bucketName: String,
                    accessNodeType: NodeType,
                    includeCredentials: Boolean = true) = {
    val s3Type = accessNodeType match {
      case NodeType.AWS => "s3a"
      case NodeType.Local => "s3a"
      case NodeType.User => GeneralUtilities.getConfStrict(
        conf, CopyConstants.sourceS3Scheme, GeneralConstants.sourceProps)
      case _ => throw new IllegalStateException("Running s3 command on an unexpected node type.")
    }

    if (includeCredentials) {
      val id = AwsUtilities.getAwsProp(conf, AwsConstants.accessId)
      val key = AwsUtilities.getAwsProp(conf, AwsConstants.accessKey)
      s"$s3Type://$id:$key@$bucketName"
    } else {
      s"$s3Type://$bucketName"
    }
  }

  def getAllFolders(conf: Map[String, String], bucketId: String, accessNode: NodeType) : Iterable[String] = {
    //TODO- here we treat all first level artifacts as folders, find a way to identify folders.
    val blobStore = BucketUtilities.getBlobStore(conf)
    val filesJava = blobStore.list(bucketId, new ListContainerOptions())
    val files = filesJava.asScala
    val filesToCopy = files.map(_.getName()).filter(!_.equals(BucketConstants.bucketLogDir))
    filesToCopy.map(f =>
      s"${getS3Location(conf, bucketId, accessNode, includeCredentials = true)}/$f")
  }
}
