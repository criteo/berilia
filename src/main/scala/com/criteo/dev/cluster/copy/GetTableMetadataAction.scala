package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster._

/**
  * Get Hive table metadata from source
  */
class GetTableMetadataAction(conf : Map[String, String], node: Node) {

  /**
    * @return first element is the HDFS location, second element is whether its partitioned,
    *         third element is the the create table DDL statement.
    */
  def apply(database : String, table : String) : (Boolean, String) = {
    val result = SshAction (node,
       s"hive --database $database -e 'show create table $table;'", returnResult = true)

     //Following is delicate and assumes a certain format.
     val partitioned = CopyUtilities.partitioned(result)
     return (partitioned, result)
   }
}
