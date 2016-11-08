package com.criteo.dev.cluster.copy

import com.criteo.dev.cluster.Node

/**
  * Copies file(s) between nodes.
  */
abstract class CopyFileAction(conf: Map[String, String], source: Node, target: Node) {


  /**
    * Copies file(s) from source to target, replicates the directory structure.
    *
    *
    * All files of sourceFiles must start with sourceBase.
    * The algorithm will copy every file to targetBase/(sourceFile - sourceBase)
    *
    * Ex: sourceFiles = /user/enginejoins/glup/click_cas/2016-08-28/1
    *                 = /user/enginejoins/glup/click_cas/2016-08-28/2
    *     sourceBase  = /user/enginejoins/glup/click_cas
    *     targetBase  = /target
    *
    *
    * will result in following files:
    *
    * /target/2016-08-28/1
    * /target/2016-08-28/2
    */


  def apply(sourceFiles: Array[String], sourceBase: String, targetBase: String)
}
