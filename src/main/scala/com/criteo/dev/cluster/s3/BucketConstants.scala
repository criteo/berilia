package com.criteo.dev.cluster.s3


/**
  * Constants for S3.
  *
  * Look in file 'AwsConstants' for other constants used by S3 API'S
  */
object BucketConstants {
  //S3 constants.  Note, S3 tags (and values) seem to auto-lowercase, so using all lower-case for these.
  def bucketPrefix = "s3.bucket.prefix"
  def bucketLogDir = "copy-log"
  def bucketTimestampFormat = "yyyy-MMM-dd-HH-mm-ss"
  def dataType = "datatype"

  def tempBucketPrefix = "temp-copy"
}
