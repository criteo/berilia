package com.criteo.dev.cluster.copy

case class TableInfo(database: String,
                     table: String,
                     location: String,
                     inputFormat: String,
                     createStmt: String,
                     partitions: Array[PartitionInfo])

case class PartitionInfo(location: String, partSpec : PartSpec)

case class PartSpec(specs: Array[PartialPartSpec])

case class PartialPartSpec(column: String, value: String)