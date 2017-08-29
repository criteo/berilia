package com.criteo.dev.cluster.source

import com.criteo.dev.cluster.copy.TableInfo

case class FullTableInfo(
                          tableInfo: TableInfo,
                          hdfsInfo: TableHDFSInfo
                        )
