package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster._
import com.criteo.dev.cluster.command.{ShellHiveAction, SshHiveAction}
import com.criteo.dev.cluster.utils.ddl.{CreateTable, DDLParser, Statement}

import scala.util.parsing.input.Position

/**
  * Get Hive table metadata from source
  */
class GetTableMetadataAction(conf: Map[String, String], node: Node, isLocalScheme: Boolean = false) {

  /**
    * @return first element is the HDFS location, second element is whether its partitioned,
    *         third element is the the create table DDL statement.
    */
  def apply(database: String, table: String): CreateTable = {
    val commands = List(s"use $database", s"show create table $table")
    val action = if (isLocalScheme) new ShellHiveAction() else new SshHiveAction(node)
    val result = {
      commands.foreach(action.add)
      action.run()
    }

    DDLParser(result) match {
      case Left((errorMsg, position)) => throw new IllegalStateException(s"Error parsing DDL: $errorMsg at $position")
      case Right(statement) => statement match {
        case c:CreateTable => c
      }
    }
  }
}
