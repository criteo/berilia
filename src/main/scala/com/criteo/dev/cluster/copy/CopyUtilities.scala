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


  def formatDdl(ddl: String, tableName: Option[String], location: Option[String]) : String = {
    var strings: Array[String] = ddl.split("\n")

    //for some reason, hive's own 'show create table statement'
    //doesn't compile and needs to be fixed like below
    strings = strings.map(s => s.trim()).map(s => s.replace("(", " ("))

    //strip tblProperties, which do not seem to parse..
    var stbuffer: scala.collection.mutable.Buffer[String] = strings.toBuffer
    val tblPropertiesIndex = stbuffer.indexWhere(s => s.startsWith("TBLPROPERTIES"))
    stbuffer = stbuffer.dropRight(stbuffer.size - tblPropertiesIndex)

    //replace create table
    if (tableName.isDefined) {
      stbuffer.remove(0)
      stbuffer.insert(0, s"CREATE TABLE ${tableName.get} (")
    }

    //replace location string
    val locationIndex = stbuffer.map(s => s.trim()).indexOf("LOCATION")
    stbuffer.remove(locationIndex + 1)
    if (location.isDefined) {
      stbuffer.insert(locationIndex + 1, s"'${location.get}'")
    } else {
      stbuffer.remove(locationIndex)
    }

    //for older versions of hive, they don't escape the column name :(
    val results = stbuffer.takeWhile(!_.startsWith("ROW FORMAT SERDE"))
      .map(s => {
        if (s.startsWith("CREATE") || s.startsWith("PARTITIONED BY")) {
          s
        } else {
          val regexString = """^\s*([^\s`]+)"""
          s.replaceAll(regexString, """`$1`""")
        }
      }) ++ stbuffer.dropWhile(!_.startsWith("ROW FORMAT SERDE"))

    //handle pail format.  Use glupInputFormat to read it as a sequenceFile.
    //The other option is
    // 1.  Copy the pail.meta file in the table's root directory.
    // 2.  Set hive.input.format = com.criteo.hadoop.hive.ql.io.PailOrCombineHiveInputFormat on the target cluster.
    //    val inputFormatIndex = stbuffer.map(s => s.trim()).indexOf("STORED AS INPUTFORMAT")
    //    val isPailif =
    //      stbuffer(inputFormatIndex + 1).toString().trim().contains("SequenceFileFormat$SequenceFilePailInputFormat")
    //    if (isPailif) {
    //      stbuffer.remove(inputFormatIndex + 1)
    //      stbuffer.insert(inputFormatIndex + 1, "  'com.criteo.hadoop.hive.ql.io.GlupInputFormat'")
    //    }

    return results.mkString(" ")
  }
}
