package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.config.GlobalConfig
import com.criteo.dev.cluster.docker.DockerCopyFileAction
import com.criteo.dev.cluster.s3.{DistCpS3Action, LocalRsyncCopyAction, ScpCopyFileAction}
import com.criteo.dev.cluster.{GeneralConstants, GeneralUtilities, Node}

/**
  * This returns different copy file depending on the mode (scp, docker-cp, distcp, etc) and configuration.
  */
object CopyFileActionFactory {

  def getCopyFileAction(config: GlobalConfig, source: Node, target: Node) : CopyFileAction = {
    // TODO: get rid of old Map config
    val conf = config.backCompat
    def copyScheme = GeneralUtilities.getConfStrict(conf, CopyConstants.sourceCopyScheme, GeneralConstants.sourceProps)
    def targetType = GeneralUtilities.getConfStrict(conf, GeneralConstants.targetTypeProp, "Internally-provided")
    targetType match {
      case (GeneralConstants.awsType) => {
        copyScheme match {
          case CopyConstants.tunnelScheme => new ScpCopyFileAction(config, source, target)
          case CopyConstants.bucketScheme => new CopyViaBucketAction(conf, source, target)
          case CopyConstants.localScheme => new LocalRsyncCopyAction(config, source, target)
          case _ => throw new IllegalArgumentException(s"Unsupported conf ${CopyConstants.sourceCopyScheme}: $copyScheme")
        }
      }
      case (GeneralConstants.localClusterType) => {
        copyScheme match {
          case CopyConstants.tunnelScheme => new DockerCopyFileAction(config, source, target)
          case CopyConstants.bucketScheme => new CopyViaBucketAction(conf, source, target)
          case CopyConstants.localScheme => new LocalRsyncCopyAction(config, source, target)
          case _ => throw new IllegalArgumentException(s"Unsupported conf ${CopyConstants.sourceCopyScheme}: $copyScheme")
        }
      }
      case (GeneralConstants.s3Type) => new DistCpS3Action(conf, source, target)
      case _ => throw new IllegalArgumentException(s"Unsupported target type: $targetType")
    }
  }
}
