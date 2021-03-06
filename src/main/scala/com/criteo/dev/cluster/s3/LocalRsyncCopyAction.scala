package com.criteo.dev.cluster.s3

import com.criteo.dev.cluster.command.{RsyncAction, ShellMultiAction, SshMultiAction}
import com.criteo.dev.cluster.config.GlobalConfig
import com.criteo.dev.cluster.copy.{CopyConstants, CopyFileAction, CopyUtilities}
import com.criteo.dev.cluster.{GeneralUtilities, Node}
import org.slf4j.LoggerFactory

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

/**
  * From local to AWS using rsync
  */
class LocalRsyncCopyAction(config: GlobalConfig, source: Node, target: Node) extends CopyFileAction(Map.empty, source, target){

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def apply(sourceFiles: Array[String], sourceBase: String, targetBase: String): Unit = {

    logger.info("Cleaning temp directories")
    GeneralUtilities.prepareTempDir
    val tempDir = GeneralUtilities.getTempDir()

    //check arguments
    sourceFiles.foreach(sf =>
      require(sf.startsWith(sourceBase), s"Internal Error, $sf does not start with $sourceBase"))

    get(tempDir, sourceFiles, sourceBase)
    copy(tempDir)
    put(tempDir, sourceFiles, sourceBase, targetBase)

    // clean up
    deleteTargetTmpDir(target)
    GeneralUtilities.cleanupTempDir
  }

  /**
    * Gets data from source HDFS (to temp)
    */
  def get(tmpDir: String, sourceFiles: Array[String], sourceBase: String) = {
    val parSourceFiles = sourceFiles.par
    logger.info(s"Getting ${sourceFiles.size} partition files, parallelism: ${config.source.parallelism.partition}")
    parSourceFiles.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(config.source.parallelism.partition))
    parSourceFiles.map { f =>
      val tmpLocationParent = getSrcTmpLocationParent(tmpDir, f, sourceBase)
      val commands = List(
        s"mkdir -p $tmpLocationParent",
        s"hdfs dfs -get $f $tmpLocationParent"
      )
      ShellMultiAction(commands)
    }
  }

  /**
    * Copies the data from source node to the target node.
    */
  def copy(srcPath: String): Unit = {
    RsyncAction(srcPath, target, CopyConstants.tmpTgtParent, false)
  }

  /**
    * Put the data (on target node) to target HDFS
    */
  def put(tmpDir: String, sourceFiles: Array[String], sourceBase: String, targetBase: String): Unit = {
    val putCommands = sourceFiles.flatMap(f => {
      val tmpLocation = getTgtTmpLocation(tmpDir, f, sourceBase)
      //don't know the name of namenode.. use relative path for HDFS
      val targetLocation = targetBase + f.stripPrefix(sourceBase)
      val targetLocationParent = CopyUtilities.getParent(targetLocation)
      List(
        s"hdfs dfs -mkdir -p $targetLocationParent",
        s"hdfs dfs -put ${if (config.source.copyConfig.overwriteIfExists) "-f" else ""} $tmpLocation $targetLocationParent"
      )
    })
    //To be idempotent, ignore errors if the file already exists
    SshMultiAction(target, putCommands.toList, ignoreError=true, returnResult=false)
  }

  def deleteTargetTmpDir(target: Node): Unit = {
    val dir = CopyConstants.tmpTgtParent + "/" + GeneralUtilities.getTempDir
    logger.info(s"Removing $dir of target")
    CopyUtilities.deleteTmp(target, dir)
  }

  def getSrcTmpLocationParent(tmpDir: String, sourceFile: String, sourceBase: String) : String = {
    val partPath = getPartPath(sourceFile, sourceBase)
    val tmpLocation = s"$tmpDir/$partPath"
    CopyUtilities.getParent(tmpLocation)
  }

  def getTgtTmpLocation(tmpDir: String, sourceFile: String, sourceBase: String) : String = {
    val partPath = getPartPath(sourceFile, sourceBase)
    s"${CopyConstants.tmpTgtParent}/$tmpDir/$partPath"
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
