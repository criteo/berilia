package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.GeneralConstants

/**
  * Constants for copy step.
  */
object CopyConstants {

  def sourceTables = "source.tables"

  def sourceFiles = "source.files"

  def copyListeners = "source.copy.listeners"

  def sampleListeners = "source.sample.listeners"

  def sampleThreshold = "source.copy.sample.threshold"

  def sampleDb = "source.sample.database"

  def sampleProb = "sample.prob"

  def sourceS3Scheme = "source.s3.hdfs.scheme"

  def tmpDataDirName = "tmpData"

  /** Temporary dir on the src machine **/
  def tmpSrc = s"~/$tmpDataDirName"

  /** Temporary dir on the tgt machine **/
  def tmpTgt = s"/tmp/$tmpDataDirName"

  def tmpTgtParent = "/tmp"

  def tempTableHint = "dev_cluster"

  def absolutePartCount = "partition.count"

  def topPartCount = "distcp.top.partition.count"

  def getAddressFull = s"source.${GeneralConstants.address}"
  def getUserFull = s"source.${GeneralConstants.user}"
  def getKeyFileFull = s"source.${GeneralConstants.keyFile}"
  def getPortFull = s"source.${GeneralConstants.port}"
}
