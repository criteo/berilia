package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.docker.DockerCopyFileAction
import com.criteo.dev.cluster.s3.{DistCpS3Action, ScpCopyFileAction}
import com.criteo.dev.cluster.{GeneralConstants, GeneralUtilities, NodeFactory}

/**
  * This returns different copy file depending on the mode (scp, docker-cp, distcp, etc)
  */
object CopyFileActionFactory {

  def getCopyFileAction(conf: Map[String, String]) : CopyFileAction = {
    def targetType = GeneralUtilities.getConfStrict(conf, GeneralConstants.targetTypeProp, "Internally-provided")
    targetType match {
      case (GeneralConstants.awsType) => new ScpCopyFileAction(conf, NodeFactory.getSource(conf), NodeFactory.getTarget(conf))
      case (GeneralConstants.localClusterType) => new DockerCopyFileAction(conf, NodeFactory.getSource(conf), NodeFactory.getTarget(conf))
      case (GeneralConstants.s3Type) => new DistCpS3Action(conf, NodeFactory.getSource(conf), NodeFactory.getTarget(conf))
      case _ => throw new IllegalArgumentException(s"Unsupported target type: $targetType")
    }
  }
}
