package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.docker.DockerCopyFileAction
import com.criteo.dev.cluster.s3.{DistCpS3Action, ScpCopyFileAction}
import com.criteo.dev.cluster.{GeneralConstants, GeneralUtilities, Node, NodeFactory}

/**
  * This returns different copy file depending on the mode (scp, docker-cp, distcp, etc) and configuration.
  */
object CopyFileActionFactory {

  def getCopyFileAction(conf: Map[String, String], source: Node, target: Node) : CopyFileAction = {
    def copyScheme = GeneralUtilities.getConfStrict(conf, CopyConstants.sourceCopyScheme, GeneralConstants.sourceProps)
    def targetType = GeneralUtilities.getConfStrict(conf, GeneralConstants.targetTypeProp, "Internally-provided")
    targetType match {
      case (GeneralConstants.awsType) => {
        copyScheme match {
          case CopyConstants.tunnelScheme => new ScpCopyFileAction(conf, source, target)
          case CopyConstants.bucketScheme => new CopyViaBucketAction(conf, source, target)
          case _ => throw new IllegalArgumentException(s"Unsupported conf ${CopyConstants.sourceCopyScheme}: $copyScheme")
        }
      }
      case (GeneralConstants.localClusterType) => {
        copyScheme match {
          case CopyConstants.tunnelScheme => new DockerCopyFileAction(conf, source, target)
          case CopyConstants.bucketScheme => new CopyViaBucketAction(conf, source, target)
          case _ => throw new IllegalArgumentException(s"Unsupported conf ${CopyConstants.sourceCopyScheme}: $copyScheme")
        }
      }
      case (GeneralConstants.s3Type) => new DistCpS3Action(conf, source, target)
      case _ => throw new IllegalArgumentException(s"Unsupported target type: $targetType")
    }
  }
}
