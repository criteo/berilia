package com.criteo.dev.cluster.s3

import com.criteo.dev.cluster._
import com.criteo.dev.cluster.copy.{CopyConstants, CopyFileAction, CopyUtilities}
import org.slf4j.LoggerFactory

/**
  * Copies over a file to S3 via distcp.
  *
  * Falls back to scp if the file is above the threshold.
  */
class DistCpS3Action(conf: Map[String, String],
                     source: Node,
                     target: Node) extends CopyFileAction(conf, source, target) {

  private val logger = LoggerFactory.getLogger(classOf[DistCpS3Action])

  override def apply(sourceFiles: Array[String], sourceBase: String, targetBase: String): Unit = {

    CopyUtilities.deleteTmpSrc(conf)
    if (fallback(sourceFiles)) {
      val scpAction = new ScpS3Action(conf, source, target)
      scpAction(sourceFiles, sourceBase, targetBase)
      return
    }

    val processedSources = processSource(sourceFiles, sourceBase)
    val distCpCmd = new StringBuilder("hadoop distcp ")
    processedSources.foreach(sf => distCpCmd.append(s"$sf "))

    val targetPath = CopyUtilities.toS3BucketTarget(conf, targetBase)
    distCpCmd.append(s"$targetPath")

    SshAction(NodeFactory.getSource(conf), distCpCmd.toString)
  }

  /**
    * Get the first level directory after the common base.
    */
  def processSource(sourceFiles: Array[String], sourceBase: String) : Array[String] = {
    val processedSources = sourceFiles.map(sf => {
      val stripped = sf.stripPrefix(sourceBase)
      val regex = "[^\\/]*".r
      regex.findFirstIn(stripped).get
    })
    processedSources.distinct.map(u => s"$sourceBase$u")
  }

  /**
    * Sometimes distcp is too much overhead, in those cases fall back to scp.
    */
  def fallback(sourceFiles: Array[String]) : Boolean = {
    if (sourceFiles.length == 1) {
      logger.info("Falling back to SCP due to limited number of files.")
      return true
    }
    return false
  }
}
