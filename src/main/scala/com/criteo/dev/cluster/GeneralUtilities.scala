package com.criteo.dev.cluster


import java.io.File
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}

import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Utilities used in all phases.
  */
object GeneralUtilities {

  private val logger = LoggerFactory.getLogger(GeneralUtilities.getClass)


  //-----
  //Support concurrency to some degree by having each command store temp files in a different temp dir
  //-----

  private val tempDir = new ThreadLocal[String]

  def getTempDir(): String = {
    if (tempDir.get == null)
      tempDir.set(s"./$getTempPrefix")
    tempDir.get
  }

  def getTempPrefix(): String = s"temp_${getSimpleDate}_thread_${Thread.currentThread.getId}"

  def getSimpleDate(): String = DateTimeFormatter
    .ofPattern("yyyyMMdd_HHmmss")
    .withZone(ZoneId.systemDefault())
    .format(Instant.now)

  def prepareDir(dir: String) = {
    val tmpDirectoryFile = new File(dir)
    FileUtils.deleteDirectory(tmpDirectoryFile)
    val success = tmpDirectoryFile.mkdir()
    if (!success) {
      throw new IllegalStateException(s"Critical error, failed to prepare directory: $dir")
    }
  }

  def prepareTempDir = prepareDir(s"${GeneralUtilities.getHomeDir}/$getTempDir")

  def cleanupTempDir = FileUtils.deleteDirectory(new File(s"${GeneralUtilities.getHomeDir}/$getTempDir"))

  def getFuture[T](body: => T): Future[T] = Future {
    prepareTempDir
    val ret = body
    cleanupTempDir
    ret
  }

  /**
    * Get a CSV from given configuration
    *
    * @param conf configuration
    * @param key  key to get in the configuration
    * @return array of values, empty if config is not defined.
    */
  def getConfCSV(conf: Map[String, String], key: String): Array[String] = {
    val stringList = getNonEmptyConf(conf, key)
    if (stringList.isDefined) {
      stringList.get.split(",").map(s => s.trim())
    } else {
      Array()
    }
  }

  def getConfCSVStrict(conf: Map[String, String], key: String, expectedFile: String): Array[String] = {
    val stringList = getConfStrict(conf, key, expectedFile)
    stringList.split(",").map(s => s.trim())
  }

  /**
    * @param conf         conf to lookup
    * @param key          configuration key
    * @param expectedFile where this conf should be defined.  It's used in the error message.
    * @return conf, or throws an exception if it is not defined.
    */
  def getConfStrict(conf: Map[String, String], key: String, expectedFile: String): String = {
    val result = conf.get(key)
    require(result.isDefined && !result.get.trim.isEmpty,
      s"Please configure $key in file [$expectedFile].")
    result.get
  }


  def getNonEmptyConf(conf: Map[String, String], key: String): Option[String] = {
    if (!conf.get(key).isDefined || conf.get(key).get.isEmpty) {
      return None
    }
    Some(conf.get(key).get)
  }

  /**
    * Used to ssh into a node, via 'ssh $result'
    *
    * @param node metadata of the node
    * @return result string
    */
  def nodeString(node: Node): String = {
    val sb = new StringBuilder()
    val nodeKey = node.key
    if (nodeKey.isDefined) {
      sb.append(s"-i ${nodeKey.get} ")
    }

    val nodePort = node.port
    if (nodePort.isDefined) {
      sb.append(s"-p ${nodePort.get} ")
    }

    val nodeUser = node.user
    val nodeIp = node.ip
    if (nodeUser.isDefined) {
      sb.append(s"${nodeUser.get}@")
    }
    sb.append(nodeIp)
    sb.toString()
  }

  /**
    * Utility to get the aux jars on the target machine.  Basically prepends /hadoop-resources (where they are copied
    * on the target machine) to the front of every jar name.
    *
    * Returns colon separated list, which will be used as Hive env var HIVE_AUX_JARS_PATH
    */
  def getAuxJarTargetList(jarList: String): String = {
    val resolvedJarList = jarList.split(",").map(_.trim()).map(j => s"${GeneralConstants.auxJarTargetDir}/$j")
    resolvedJarList.mkString("\"", ":", "\"")
  }


  /**
    * Checks exit code for an external command.
    */
  def checkStatus(returnCode: Int): Unit = {
    returnCode match {
      case 0 => return
      case _ => throw new IllegalStateException(s"Command returned exit code: $returnCode")
    }
  }

  def getDataDir(mounts: List[String]): String = {
    if (!mounts.isEmpty) {
      mounts.map(m => s"""file://$m""").mkString(", ")
    } else {
      """file://${hadoop.tmp.dir}/dfs/data"""
    }
  }

  //----
  // Support the case of running against the API on a berilia install.
  //----

  private val envvar = System.getenv(GeneralConstants.homeSysVar)
  if (envvar != null) {
    logger.info(s"${GeneralConstants.homeSysVar} set to $envvar")
  }

  def getHomeDir = {
    val envvar = System.getenv(GeneralConstants.homeSysVar)
    if (envvar == null || envvar.isEmpty) {
      "."
    } else {
      s"$envvar"
    }
  }
}


