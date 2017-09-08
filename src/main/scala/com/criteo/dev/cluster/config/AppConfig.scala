package com.criteo.dev.cluster.config

import com.criteo.dev.cluster.Public
import com.criteo.dev.cluster.copy.CopyConstants

@Public
case class AppConfig(
                      address: String,
                      tables: List[TableConfig],
                      copyConfig: CopyConfig,
                      s3HDFSScheme: String,
                      defaultSampleProb: Double,
                      defaultPartitionCount: Int,
                      defaultSampleSize: Option[Long],
                      sampleDatabase: String,
                      parallelism: ParallelismConfig,
                      gateway: GatewayConfig,
                      environment: EnvironmentConfig,
                      local: LocalConfig,
                      aws: AWSConfig
                    ) {
  lazy val isLocalScheme = copyConfig.scheme == CopyConstants.localScheme
  lazy val isTunnelScheme = copyConfig.scheme == CopyConstants.tunnelScheme
  lazy val isBucketScheme = copyConfig.scheme == CopyConstants.bucketScheme
}

/**
  * Table config
  *
  * @param name           the name of the table
  * @param sampleProb     the sampling probability
  * @param sampleSize     the sampling size, which overrides the sample prob
  * @param partitions     the list of partitions
  * @param partitionCount the number of partitions to be copied
  */
case class TableConfig(
                        name: String,
                        sampleProb: Option[Double],
                        sampleSize: Option[Long],
                        partitions: List[List[String]],
                        partitionCount: Option[Int],
                        skipCleanup: Boolean
                      )

case class CopyConfig(
                       scheme: String,
                       sampleThreshold: Long,
                       overwriteIfExists: Boolean,
                       listeners: List[String],
                       sampleListeners: List[String]
                     )

case class GatewayConfig(
                          dockerPorts: List[String],
                          dockerFiles: List[String],
                          conf: Map[String, String]
                        )

case class ParallelismConfig(
                              table: Int,
                              partition: Int
                            )

case class EnvironmentConfig(
                              hadoopConfDir: String,
                              hiveAuxJars: List[String],
                              hadoopVersion: String,
                              baseOS: String
                            )

case class LocalConfig(
                        clusterUser: String,
                        ports: List[String],
                        dockerFiles: List[String],
                        dockerContainerId: String
                      )

case class AWSConfig(
                      accessId: String,
                      accessKey: String,
                      instanceType: String,
                      volumeSpec: VolumeSpec,
                      autoVolumes: Boolean,
                      user: String,
                      region: String,
                      subnet: String,
                      securityGroup: String,
                      keyPair: String,
                      keyFile: String,
                      baseImageId: String,
                      s3BucketPrefix: String
                    )

@Public
case class VolumeSpec(
                       master: List[Volume],
                       slave: List[Volume]
                     )

case class Volume(
                   name: String,
                   size: Int // in GB
                 )
