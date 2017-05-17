package com.criteo.dev.cluster.config

case class TargetConfig(
                         common: CommonConfig,
                         local: LocalConfig,
                         aws: AWSConfig
                       )

case class CommonConfig(
                         hadoopConfDir: String,
                         hiveAuxJars: List[String],
                         hadoopVersion: String,
                         baseOS: String
                       )

case class LocalConfig(
                        clusterUser: String,
                        ports: List[String],
                        dockerFiles: List[String]
                      )

case class AWSConfig(
                      accessId: String,
                      accessKey: String,
                      instanceType: String,
                      volumeSpec: String,
                      user: String,
                      region: String,
                      subnet: String,
                      securityGroup: String,
                      keyPair: String,
                      keyFile: String,
                      baseImageId: String,
                      s3BucketPrefix: String
                    )
