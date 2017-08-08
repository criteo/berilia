package com.criteo.dev.cluster.s3

import com.criteo.dev.cluster.command.{ScpAction, SshMultiAction}
import com.criteo.dev.cluster.copy.{CleanupAction, CopyConstants, CopyFileAction, CopyUtilities}
import com.criteo.dev.cluster.Node
import org.slf4j.LoggerFactory

/**
  * Use scp to copy over a file.
  *
  * This might be replaced with some HDFS calls, but trouble is getting the right version
  * of Hadoop for source/target cluster, and setting up network and Kerberos on target machine to
  * access the source...
  * <p>
  * For now, it gets the sample HDFS data on the gateway, scp's it over to the target,
  * then puts it in the target hdfs.
  */
class ScpCopyFileAction(conf: Map[String, String], source: Node, target: Node)
  extends CopyFileAction(conf, source, target) {

  private val logger = LoggerFactory.getLogger(classOf[ScpCopyFileAction])

  override def apply(sourceFiles: Array[String], sourceBase: String, targetBase: String): Unit = {

    logger.info("Cleaning temp directories")
    CleanupAction(conf, source, target)

    //check arguments
    sourceFiles.foreach(sf =>
      require(sf.startsWith(sourceBase), s"Internal Error, $sf does not start with $sourceBase"))

    get(sourceFiles, sourceBase)
    copy()
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
    * Copies the data from source node to the target node.
    */
  def copy(): Unit = {
    ScpAction(Some(source), CopyConstants.tmpSrc, Some(target), CopyConstants.tmpTgtParent)
  }

  /**
    * Put the data (on target node) to target HDFS
    */
  def put(sourceFiles: Array[String], sourceBase: String, targetBase: String): Unit = {
    val putCommands = sourceFiles.flatMap(f => {
      val tmpLocation = getTgtTmpLocation(f, sourceBase)
      //don't know the name of namenode.. use relative path for HDFS
      val targetLocation = targetBase + f.stripPrefix(sourceBase)
      val targetLocationParent = CopyUtilities.getParent(targetLocation)
      s"hdfs dfs -mkdir -p $targetLocationParent" ::
        s"hdfs dfs -put $tmpLocation $targetLocationParent" :: Nil
    })
    //To be idempotent, ignore errors if the file already exists
    SshMultiAction(target, putCommands.toList, ignoreError=true, returnResult=false)
  }


  def getSrcTmpLocationParent(sourceFile: String, sourceBase: String) : String = {
    val partPath = getPartPath(sourceFile, sourceBase)
    val tmpLocation = s"${CopyConstants.tmpSrc}/$partPath"
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
