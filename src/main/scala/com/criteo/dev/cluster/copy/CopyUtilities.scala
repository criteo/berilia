package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.aws.{AwsConstants, AwsUtilities}
import com.criteo.dev.cluster.{GeneralConstants, GeneralUtilities, NodeFactory, SshAction}

import scala.util.Random

/**
  * Utilities for copying metadata.
  */
object CopyUtilities {

  //For configuration stuff

  def getOverridableConf(conf: Map[String, String], database: String, table: String, prop: String) : String = {
    val propConf = conf.get(s"$database.$table.$prop")
    if (propConf.isEmpty) {
      GeneralUtilities.getConfStrict(conf, s"default.$prop", GeneralConstants.sourceProps)
    } else {
      propConf.get
    }
  }


  /**
    * Annoyingly, "get dir1 dir1" will make dir1/dir1
    * Similarly, "put dir1 dir1" will make dir1/dir1
    * So we have to do "get/put dir1 dirParent" to get the path right.
    */
  def getParent(dir: String): String = {
    dir.replaceAll("\\/[^\\/]*$", "")
  }

  def deleteTmpSrc(conf: Map[String, String]) =
    //cleanup of tmp dir on source
    SshAction(NodeFactory.getSource(conf), "rm -rf " + CopyConstants.tmpSrc, ignoreFailure = true)


  def deleteTmpTgt(conf: Map[String, String]) =
    SshAction(NodeFactory.getTarget(conf), "rm -rf " + CopyConstants.tmpTgt, ignoreFailure = true)

  /**
    * A little delicate and assumes a certain 'create table' format, like
    * ...
    * 'DESCRIPTOR'
    *   'property'
    * ...
    *
    * TODO- replace with metadata client calls if appropriate.
    */
  def property(descriptor: String) (ddl: String) : String = {
    val lines = ddl.split("\n")
    val index = lines.map(s => s.trim()).indexOf(descriptor)
    lines(index + 1).replaceAll("'", "").trim()
  }

  def location = property("LOCATION") _
  def inputFormat = property("STORED AS INPUTFORMAT") _


  def partitioned(ddl : String): Boolean = {
    val ddlLines = ddl.split("\n").map(s => s.trim())
    ddlLines.exists(s => s.trim().startsWith("PARTITIONED BY"))
  }

  def toRelative(srcLocation: String) : String = {
    //Strips the hdfs://<namenode-url> part of the path.
    srcLocation.replaceAll("^(hdfs:\\/\\/)[^\\/]*", "")
  }


  def toS3BucketTarget(conf: Map[String, String], path: String, includeCredentials: Boolean = true) = {
    val target = NodeFactory.getTarget(conf)
    val bucketName = target.ip
    if (includeCredentials) {
      val id = AwsUtilities.getAwsProp(conf, AwsConstants.accessId)
      val key = AwsUtilities.getAwsProp(conf, AwsConstants.accessKey)
      s"s3a://$id:$key@$bucketName$path"
    } else {
      s"s3a://$bucketName$path"
    }
  }

  /**
    * Convert from part spec string, ie (day='2015-05-05', hour='20')
    * to modeled partition spec.
    *
    * @param partSpec
    */
  def getPartInfos(partSpec: String): PartSpec = {
    val partSpecs = partSpec.split("/")
    PartSpec(partSpecs.map { s =>
      val splitted = s.split("=")
      PartialPartSpec(splitted(0), splitted(1))
    })
  }

  /**
    * @param partSpecs array of the the partition specs (column, value) of this partition.
    *
    * @return partition spec string, ie (day='2015-05-05', hour='20') for each partition.
    */
  def partitionSpecString(partSpecs : PartSpec): String = {
    val partSpecStrings = partSpecs.specs.map(
      p => {
        s"${p.column}='${p.value}'"
      })
    partSpecStrings.mkString(", ")
  }

  /**
    * @param partSpecs array of the the partition specs (column, value) of this partition.
    *
    * @return partition spec filter, ie (day='2015-05-05' and hour='20')
    */
  def partitionSpecFilter(partSpecs : PartSpec): String = {
    val partSpecStrings = partSpecs.specs.map(
      p => {
        s"${p.column}='${p.value}'"
      })
    partSpecStrings.mkString(" and ")
  }

  /**
    * @param part partition info
    *
    * @return partition column list of this table, ie (day, hour)
    */
  def partitionColumns(part: PartitionInfo) : String = {
    part.partSpec.specs.map(_.column).mkString("(", ", ", ")")
  }


  /**
    * Get a temp table name, for sampling purpose.
    */
  def getTempTableName(tableName: String) : String = {
    val rand = Random.nextInt(10000).toString
    s"${tableName}_${CopyConstants.tempTableHint}_$rand"
  }

  /**
    * Get a common file location to copy for.
    */
  def getCommonLocation(tableLocation: String, partitionInfos: Array[PartitionInfo]): String = {
    if (partitionInfos.isEmpty) {
      tableLocation
    } else {
      val partLocations = partitionInfos.map(_.location)
      // getCommonLocation(partLocations)

      partitionInfos.foreach(p => {
        //TODO - support this case, find the common parent of partitions if not under table directory,
        //will work with the following code.
        require(p.location.startsWith(tableLocation))
      })
      tableLocation
    }
  }
}
