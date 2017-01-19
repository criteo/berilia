package com.criteo.dev.cluster.docker

import com.criteo.dev.cluster.{GeneralConstants, GeneralUtilities, Node, ScpAction}
import com.criteo.dev.cluster.copy._
import com.criteo.dev.cluster.s3.ScpCopyFileAction
import org.slf4j.LoggerFactory

/**
  * Main change is to use scp + docker cp instead of scp -r,
  * as that doesnt work on local docker instances where ssh port is not 22
  */
class DockerCopyFileAction(conf: Map[String, String],
                           source: Node,
                           target: Node) extends ScpCopyFileAction(conf, source, target) {



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
      conf.get(DockerConstants.localContainerId).get,  //hack, should be in node type..
      CopyConstants.tmpTgtParent)
    GeneralUtilities.cleanupTempDir
  }
}