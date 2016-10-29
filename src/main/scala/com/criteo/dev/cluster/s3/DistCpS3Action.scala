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

    val processedSources = processSource(sourceFiles, sourceBase)
    if (fallback(sourceFiles, processedSources)) {
      val scpAction = new ScpS3Action(conf, source, target)
      scpAction(sourceFiles, sourceBase, targetBase)
      return
    }


    val distCpCmd = new StringBuilder("hadoop distcp ")
    processedSources.foreach(sf => distCpCmd.append(s"$sf "))

    val targetPath = CopyUtilities.toS3Bucket(conf, targetBase)
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
    * Sometimes distcp is too much overhead or does not work.
    *
    * In these cases, fall back to scp.
    */
  def fallback(sourceFiles: Array[String], processedSources: Array[String]) : Boolean = {
    if (sourceFiles.length == 1) {
      logger.info("Falling back to SCP due to limited number of files.")
      return true
    }

    val listAction = new SshMultiAction(NodeFactory.getSource(conf))
    processedSources.foreach(sf => {
      logger.info(s"Checking directory for possibility of distcp: $sf")
      listAction.add(s"hdfs dfs -ls -R $sf")
    })

    val distcpThresholdConf = GeneralUtilities.getConfStrict(conf, CopyConstants.distcpThreshold, GeneralConstants.sourceProps)
    val distcpThreshold = Integer.valueOf(distcpThresholdConf)
    val list = listAction.run(returnResult = true).split("\n")
    list.foreach(l => {
      val fileInfo = l.split("\\s+")
      if (fileInfo.length > 7) {
        val fileName = fileInfo(7)
        val size = fileInfo(4)
        logger.info(s"Size=$size, file=$fileName")
        if (Integer.valueOf(size) > distcpThreshold) {
          logger.info(s"Falling back because of large file size: $fileName: $size")
          return true
        }
      }
    })

    processedSources.foreach(sf => logger.info(s"OK to use distcp: $sf"))
    return false
  }
}
