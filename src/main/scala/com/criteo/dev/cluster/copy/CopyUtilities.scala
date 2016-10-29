package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.aws.{AwsConstants, AwsUtilities}
import com.criteo.dev.cluster.{NodeFactory, SshAction}

/**
  * Utilities for copying metadata.
  */
object CopyUtilities {


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


  def toS3Bucket(conf: Map[String, String], path: String, includeCredentials: Boolean = true) = {
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
    * partition spec string, ie (day='2015-05-05', hour='20') for each partition.
    *
    * @param partSpecs array of the the partition specs (column, value) of this partition.
    */
  def partitionSpecString(partSpecs : PartSpec): String = {
    val partSpecStrings = partSpecs.specs.map(
      p => {
        s"${p.column}='${p.value}'"
      })
    partSpecStrings.mkString(", ")
  }
}
