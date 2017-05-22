package com.criteo.dev.cluster.config

case class SourceConfig(
                         address: String,
                         tables: List[TableConfig],
                         copyConfig: CopyConfig,
                         s3HDFSScheme: String,
                         defaultSampleProb: Double,
                         defaultPartitionCount: Int,
                         sampleDatabase: String,
                         gateway: GatewayConfig
                       )

case class TableConfig(
                        name: String,
                        sampleProb: Option[Double],
                        partitions: List[List[String]]
                      )

case class CopyConfig(
                       scheme: String,
                       sampleThreshold: Long,
                       listeners: List[String],
                       sampleListeners: List[String]
                     )

case class GatewayConfig(
                          dockerPorts: List[String],
                          dockerFiles: List[String],
                          conf: Map[String, String]
                        )
