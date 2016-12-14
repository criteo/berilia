package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster._
import com.criteo.dev.cluster.utils.ddl.{CreateTable, DDLParser, Statement}

import scala.util.parsing.input.Position

/**
  * Get Hive table metadata from source
  */
class GetTableMetadataAction(conf: Map[String, String], node: Node) {

  /**
    * @return first element is the HDFS location, second element is whether its partitioned,
    *         third element is the the create table DDL statement.
    */
  def apply(database: String, table: String): CreateTable = {
    val result = SshAction(node,
      s"hive --database $database -e 'show create table $table;'", returnResult = true)


    DDLParser(result) match {
      case Left((errorMsg, position)) => throw new IllegalStateException(s"Error parsing DDL: $errorMsg at $position")
      case Right(statement) => statement match {
        case c:CreateTable => c
      }
    }
  }
}
