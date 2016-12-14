package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.utils.ddl.CreateTable

case class TableInfo(database: String,
                     table: String,
                     ddl : CreateTable,
                     partitions: Array[PartitionInfo])

case class PartitionInfo(location: String, partSpec : PartSpec)

case class PartSpec(specs: Array[PartialPartSpec])

case class PartialPartSpec(column: String, value: String)