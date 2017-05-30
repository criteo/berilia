package com.criteo.dev.cluster.source

import com.criteo.dev.cluster.copy.TableInfo

case class SourceTableInfo(
                            tableInfo: TableInfo,
                            hdfsInfo: TableHDFSInfo
                          )
