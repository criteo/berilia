package com.criteo.dev.cluster.source

import com.criteo.dev.cluster.{Node, SshAction}

import scala.annotation.tailrec

object HDFSUtils {

  // max number of files can be put into a single dfs -du command, limit needed due to shell command max args size
  private val DuMaxFileNumber = 1024

  /**
    * Get the list of file sizes, given a list of HDFS file locations
    *
    * @param locations List of file locations
    * @param node      The node
    * @return List of file sizes
    */
  def getFileSize(locations: List[String], node: Node): List[Long] =
    splitList(locations, DuMaxFileNumber)
      .map(chunk =>
        SshAction
          .apply(node, s"hdfs dfs -du -s ${chunk.mkString(" ")}", true)
          .split("\n")
          .map(_.split(" ").head.toLong)
      )
      .flatten

  // split a list into chunks with the give chunk size
  def splitList[A](in: List[A], chunkSize: Int): List[List[A]] = {
    @tailrec
    def helper(in: List[A], acc: List[List[A]]): List[List[A]] = in match {
      case Nil => acc
      case xs =>
        val (chunk, rest) = xs.splitAt(chunkSize)
        helper(rest, chunk :: acc)
    }

    helper(in, List.empty).reverse
  }
}
