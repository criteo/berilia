package com.criteo.dev.cluster.source

import org.scalatest.{FlatSpec, Matchers}

class HDFSUtilsSpec extends FlatSpec with Matchers {

  "splitList" should "split a list into chunks" in {
    val res = HDFSUtils.splitList((1 to 19).toList, 5)
    res shouldEqual List(List(1, 2, 3, 4, 5), List(6, 7, 8, 9, 10), List(11, 12, 13, 14, 15), List(16, 17, 18, 19))
  }
}
