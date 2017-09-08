package com.criteo.dev.cluster.docker

import com.criteo.dev.cluster.command.ScpAction
import com.criteo.dev.cluster.config.GlobalConfig
import com.criteo.dev.cluster.{GeneralConstants, GeneralUtilities, Node}
import com.criteo.dev.cluster.copy._
import com.criteo.dev.cluster.s3.ScpCopyFileAction
import org.slf4j.LoggerFactory

/**
  * Main change is to use scp + docker cp instead of scp -r,
  * as that doesnt work on local docker instances where ssh port is not 22
  */
class DockerCopyFileAction(config: GlobalConfig,
                           source: Node,
                           target: Node) extends ScpCopyFileAction(config, source, target) {



  override def copy() = {
    val localTmpDir = GeneralUtilities.getTempDir
    GeneralUtilities.prepareTempDir
    ScpAction(
      Some(source),
      CopyConstants.tmpSrc,
      None,
      localTmpDir)

    DockerCpAction(
      s"$localTmpDir/${CopyConstants.tmpDataDirName}",
      config.app.local.dockerContainerId,
      CopyConstants.tmpTgtParent)
    GeneralUtilities.cleanupTempDir
  }
}