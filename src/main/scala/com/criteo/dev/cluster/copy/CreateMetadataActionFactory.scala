package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.s3.SaveS3MetadataAction
import com.criteo.dev.cluster.{GeneralConstants, GeneralUtilities, Node}

/**
  * Most of the cases, we create the metadata of the copied tables on the cluster.
  *
  * In S3 case (shared data/metadata), we save it on S3 itself.
  */
object CreateMetadataActionFactory {
  def getCopyFileAction(conf: Map[String, String], target: Node) : CreateMetadataAction = {
    def targetType = GeneralUtilities.getConfStrict(conf, GeneralConstants.targetTypeProp, "Internally-provided")
    targetType match {
      case (GeneralConstants.awsType) => new CreateMetadataHiveAction(conf, target)
      case (GeneralConstants.localClusterType) => new CreateMetadataHiveAction(conf, target)
      case (GeneralConstants.s3Type) => new SaveS3MetadataAction(conf, target)
      case _ => throw new IllegalArgumentException(s"Unsupported target type: $targetType")
    }
  }
}
