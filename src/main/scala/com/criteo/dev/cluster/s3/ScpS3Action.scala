package com.criteo.dev.cluster.s3

import com.criteo.dev.cluster._
import com.criteo.dev.cluster.copy.{CopyConstants, CopyFileAction, CopyUtilities}

/**
  * For some specific cases, use scp instead of distcp.
  */
class ScpS3Action(conf: Map[String, String],
                  source: Node,
                  target: Node) extends CopyFileAction(conf, source, target) {

  override def apply(sourceFiles: Array[String], sourceBase: String, targetBase: String): Unit = {

    //check arguments
    sourceFiles.foreach(sf =>
      require(sf.startsWith(sourceBase), s"Internal Error, $sf does not start with $sourceBase"))

    get(sourceFiles, sourceBase)
    put(sourceFiles, sourceBase, targetBase)
  }

  /**
    * Gets data from source HDFS (to temp)
    */
  def get(sourceFiles: Array[String], sourceBase: String) = {
    val getCommands = sourceFiles.flatMap(f => {
      val tmpLocationParent = getSrcTmpLocationParent(f, sourceBase)
      s"mkdir -p $tmpLocationParent" ::
        s"hdfs dfs -get $f $tmpLocationParent" :: Nil
    })
    SshMultiAction(source, getCommands.toList)
  }

  /**
    * Put the data (on target node) to target HDFS
    */
  def put(sourceFiles: Array[String], sourceBase: String, targetBase: String): Unit = {
    val putCommands = sourceFiles.flatMap(f => {
      val tmpLocation = getSrcTmpLocation(f, sourceBase)
      //don't know the name of namenode.. use relative path for HDFS
      val relPath = CopyUtilities.toRelative(f)
      val targetLocation = CopyUtilities.toS3Bucket(conf, relPath)
      val targetLocationParent = CopyUtilities.getParent(targetLocation)

      s"hdfs dfs -mkdir -p $targetLocationParent" ::
        s"hdfs dfs -put $tmpLocation $targetLocationParent" :: Nil
    })
    //To be idempotent, ignore errors if the file already exists
    SshMultiAction(source, putCommands.toList, ignoreError=true, returnResult=false)
  }

  def getSrcTmpLocation(sourceFile: String, sourceBase: String) : String = {
    val partPath = getPartPath(sourceFile, sourceBase)
    s"${CopyConstants.tmpSrc}/$partPath"
  }

  def getSrcTmpLocationParent(sourceFile: String, sourceBase: String) : String = {
    val tmpLocation = getSrcTmpLocation(sourceFile, sourceBase)
    CopyUtilities.getParent(tmpLocation)
  }

  def getTgtTmpLocation(sourceFile: String, sourceBase: String) : String = {
    val partPath = getPartPath(sourceFile, sourceBase)
    s"${CopyConstants.tmpTgt}/$partPath"
  }

  def getPartPath(sourceFile: String, sourceBase: String) : String = {
    //include the sourceBase as well, to have a containing directory under the $tmpDir.
    //ex, if sourceBase == ../table
    // => partPath = /table/part...
    // => tmp      = $tmpDir/table/part...
    val sourceBaseParent = CopyUtilities.getParent(sourceBase)
    sourceFile.substring(sourceBaseParent.length() + 1)
  }
}