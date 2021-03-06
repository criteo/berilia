package com.criteo.dev.cluster.config

import com.criteo.dev.cluster.Public

@Public
case class TargetConfig(
                         common: CommonConfig,
                         local: LocalConfig,
                         aws: AWSConfig
                       )

@Public
case class CommonConfig(
                         hadoopConfDir: String,
                         hiveAuxJars: List[String],
                         hadoopVersion: String,
                         baseOS: String
                       )

@Public
case class LocalConfig(
                        clusterUser: String,
                        ports: List[String],
                        dockerFiles: List[String],
                        dockerContainerId: String
                      )

@Public
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
