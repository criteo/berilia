package com.criteo.dev.cluster

import java.io.File

import org.slf4j.LoggerFactory

import scala.sys.process.{Process, ProcessBuilder, ProcessCreation}


/**
  * To allow for calling DevCluster API, we need to run the commands from the dev-cluster installation,
  * which looks for set locations for /conf, /docker, /hadoop-resources, etc.
  */
object DevClusterProcess {

  private val logger = LoggerFactory.getLogger(DevClusterProcess.getClass)

  val homeDirFile = new File(GeneralUtilities.getHomeDir)
  if (!homeDirFile.exists() || !homeDirFile.isDirectory) {
    logger.warn(s"${homeDirFile.getAbsolutePath} does not exist or is not a directory, exiting")
    System.exit(1)
  }

  def process(command: String, skipLog: Boolean = false): ProcessBuilder = {
    if (!skipLog) {
      logger.info(command)
    }
    Process(command, homeDirFile)
  }

  def processSeq(command: Seq[String], skipLog: Boolean = false): ProcessBuilder = {
    if (!skipLog) {
      logger.info(command.mkString(" "))
    }
    Process(command, homeDirFile)
  }
}
