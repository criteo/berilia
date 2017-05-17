package com.criteo.dev.cluster.config

/**
  * Source config converter
  * For the backward compatibility of existing code
  */
object SourceConfigConverter {
  def apply(config: SourceConfig): Map[String, String] = {
    Map(
      "source.address" -> config.address,
      "source.tables" -> config.tables.map(t => t.name + t.partitions.map(_.mkString(" (", ",", ")")).mkString).mkString(";"),
      "default.partition.count" -> config.defaultPartitionCount.toString,
      "default.sample.prob" -> config.defaultSampleProb.toString,
      "source.sample.database" -> config.sampleDatabase,
      "source.copy.listeners" -> config.copyConfig.listeners.mkString(","),
      "source.sample.listeners" -> config.copyConfig.sampleListeners.mkString(","),
      "source.copy.sample.threshold" -> config.copyConfig.sampleThreshold.toString,
      "source.s3.hdfs.scheme" -> config.s3HDFSScheme,
      "source.copy.scheme" -> config.copyConfig.scheme,
      "gateway.docker.files" -> config.gateway.dockerFiles.mkString(","),
      "gateway.docker.ports" -> config.gateway.dockerPorts.mkString(",")
    ) ++ {
      // table.sample.prob -> number
      config.tables.collect {
        case TableConfig(name, Some(sampleProb), _) => (s"$name.sample.prob", sampleProb.toString)
      }
    } ++ config.gateway.conf.map { case (k, v) => (s"gateway.$k", v) }
  }
}
