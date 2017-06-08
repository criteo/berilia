package com.criteo.dev.cluster.source

case class TableHDFSInfo(
                          database: String,
                          table: String,
                          size: Long,
                          files: List[HDFSFileInfo],
                          partitions: Int
                        )

case class InvalidTable(
                         name: String,
                         input: String,
                         message: String
                       )

case class HDFSFileInfo(
                         path: String,
                         size: Long
                       )
