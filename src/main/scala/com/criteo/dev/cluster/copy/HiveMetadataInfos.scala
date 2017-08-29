package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.utils.ddl.CreateTable

case class TableInfo(
                      database: String, //optional in the ddl
                      name: String,
                      ddl: CreateTable,
                      partitions: Array[PartitionInfo]
                    ) {
  lazy val fullName = s"$database.$name"
}

case class PartitionInfo(location: String, partSpec: PartSpec)

case class PartSpec(specs: Array[PartialPartSpec])

case class PartialPartSpec(column: String, value: String)