package com.criteo.dev.cluster.s3

import com.criteo.dev.cluster.Public
import com.criteo.dev.cluster.s3.DataType.DataType

/**
  * Represents an S3 bucket.
  */
@Public case class BucketMeta(name: String)


/**
  * Represents a description of a bucket (includes the data of a bucket).  Returned from "describe-bucket"
  */
@Public case class BucketDataMeta(bucketName: String, dataBlocks: List[BucketDataBlockMeta])


/**
  * Represents a set of data copied to a bucket via one call to "copy-bucket".
  */
@Public case class BucketDataBlockMeta(writer: String, filteredListing: List[String], date: String, dataType: DataType)

@Public object DataType extends Enumeration {
  type DataType = Value
  val hive, hdfs = Value
}
