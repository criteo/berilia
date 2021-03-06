package com.criteo.dev.cluster.s3

import com.criteo.dev.cluster.command.SshMultiAction
import com.criteo.dev.cluster.{command, _}
import com.criteo.dev.cluster.copy.{CopyConstants, CopyFileAction, CopyUtilities}

/**
  * For some specific cases, use scp instead of distcp.
  *
  * Scp a file to or from S3.
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

    val sourceFileFullPaths = sourceFiles.map(sf => {
      source.nodeType match {
        case NodeType.S3 => BucketUtilities.toS3Location(conf, target.ip, sf, target.nodeType, includeCredentials=true)
        case _ => sf
      }
    })

    val getCommands = sourceFileFullPaths.flatMap(f => {
      val tmpLocationParent = getSrcTmpLocationParent(f, sourceBase)
      s"mkdir -p $tmpLocationParent" ::
        s"hdfs dfs -get $f $tmpLocationParent" :: Nil
    })
    command.SshMultiAction(source, getCommands.toList)
  }


  /**
    * Put the data (on target node) to target HDFS
    */
  def put(sourceFiles: Array[String], sourceBase: String, targetBase: String): Unit = {

    val sshMultiAction = new SshMultiAction(source)

    sourceFiles.foreach(f => {
      val tmpLocation = getSrcTmpLocation(f, sourceBase)

      val targetLocation = {
        val tgtLocation = s"$targetBase/${CopyUtilities.getPartPath(f, sourceBase, includeBase=false)}"
        target.nodeType match {
          case NodeType.S3 => BucketUtilities.toS3Location(conf, target.ip, tgtLocation, source.nodeType, includeCredentials = true)
          case _ => tgtLocation
        }
      }
      val targetLocationParent = CopyUtilities.getParent(targetLocation)
      sshMultiAction.add(s"hdfs dfs -mkdir -p $targetLocationParent")
      sshMultiAction.add(s"hdfs dfs -put $tmpLocation $targetLocationParent")
    })

    //To be idempotent, ignore errors if the file already exists
    sshMultiAction.run(ignoreError=true, returnResult=true)
  }

  def getSrcTmpLocation(sourceFile: String, sourceBase: String) : String = {
    val partPath = CopyUtilities.getPartPath(sourceFile, sourceBase, includeBase = true)
    s"${CopyConstants.tmpSrc}/$partPath"
  }

  def getSrcTmpLocationParent(sourceFile: String, sourceBase: String) : String = {
    val tmpLocation = getSrcTmpLocation(sourceFile, sourceBase)
    CopyUtilities.getParent(tmpLocation)
  }

  def getTgtTmpLocation(sourceFile: String, sourceBase: String) : String = {
    val partPath = CopyUtilities.getPartPath(sourceFile, sourceBase, includeBase = true)
    s"${CopyConstants.tmpTgt}/$partPath"
  }
}