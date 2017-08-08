package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster._
import com.criteo.dev.cluster.command.{ShellHiveAction, SshHiveAction}
import com.criteo.dev.cluster.utils.ddl.CreateTable

/**
  * Get the partitions of a table.
  */
class GetPartitionMetadataAction (conf: Map[String, String], node: Node, isLocalScheme: Boolean = false) {

  def apply(database : String,
            table : String,
            ddl: CreateTable,
            partitions : Array[PartSpec]): Array[PartitionInfo] = {
    if (partitions.isEmpty) {
      return Array.empty[PartitionInfo]
    }

    val action = if (isLocalScheme) new ShellHiveAction() else new SshHiveAction(node)
    partitions.foreach { p =>
      action.add(s"describe formatted $database.$table partition (${CopyUtilities.partitionSpecString(p,
        ddl.partitionedBy)})")
    }
    val result = action.run()
    val splitResults = result.split("\n")

    //Decliately scraping result, which will be like:
    //...
    //Location          hdfs://<nn>/<path>
    //...
    //Here, we try to get the second part by splitting on the whitespace.
    val locations = for {
      p <- splitResults if p.trim.startsWith("Location")
    } yield p.split("\\s+")(1).trim

    require(partitions.length == locations.length)
    locations.zip(partitions).map { case (location, partSpec) =>
      PartitionInfo(location, partSpec)
    }
  }
}
