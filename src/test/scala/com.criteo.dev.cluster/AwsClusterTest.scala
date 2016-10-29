package com.criteo.dev.cluster

import com.criteo.dev.cluster.aws._
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  * This will only run if ./conf/target-aws.xml is configured with a valid AWS account.
  *
  * So it is not enabled in the actual suite.
  */
class AwsClusterTest extends FunSuite with BeforeAndAfter {
  val conf = ConfigManager.load(List())

  def testDbName = "testdb"
  def testTableName = "testtable"
  def testFileName = "testfile"
  def currentUser = System.getenv("USER")

  var clusterId: String = null

  before {
  }

  test("Create a cluster, list it") {

    //Create a docker cluster
    val cluster = CreateCompleteClusterCliAction(List("3"), conf)
    clusterId = cluster.master.id

    assertResult(2) (cluster.slaves.size)
    assertResult(AwsRunning) (cluster.master.status)
    assertResult(currentUser) (cluster.master.user)

    //List and make sure it is returned
    val clusters = ListAwsCliAction(List(), conf)
    val fetchedCluster = getCluster(clusterId, clusters)

    assertResult(2) (fetchedCluster.slaves.size)
    assertResult(AwsRunning) (fetchedCluster.master.status)
    assertResult(currentUser) (fetchedCluster.master.user)
  }

  test("Extend cluster time, should error after the second time") {
    var clusters = ListAwsCliAction(List(), conf)
    var cluster = getCluster(clusterId, clusters)
    val originalTime = AwsUtilities.stringToDt(cluster.master.expireTime)

    ExtendAwsCliAction(List(clusterId), conf)
    clusters = ListAwsCliAction(List(), conf)
    cluster = getCluster(clusterId, clusters)
    val extendedTime = AwsUtilities.stringToDt(cluster.master.expireTime)

    assert(extendedTime.isAfter(originalTime), "Extension did not extend the time")

    assertThrows[IllegalArgumentException] (ExtendAwsCliAction(List(clusterId), conf),
      "Exception expected as cannot extend the expire time twice.")
  }

  test("Create test database, table, partitions in Node, Run a query") {

    //List and get the created AWS cluster
    val awsClusters = ListAwsCliAction(List(), conf)
    val clusterMeta = getCluster(clusterId, awsClusters)

    //Run some Hive commands on the AWS cluster master.
    val master = NodeFactory.getAwsNode(conf, clusterMeta.master)

    //create database, verify
    SshHiveAction(master, List(s"create database $testDbName"))
    val result = SshHiveAction(master, List("show databases"))
    assert(result.contains(testDbName))

    //create table, verify
    SshHiveAction(master, List(s"create table $testDbName.$testTableName (name string) partitioned by (month int, day int)"))
    val resultTables = SshHiveAction(master, List(
      s"use $testDbName",
      s"show tables"))
    assert(resultTables.contains(testTableName))

    //create 4 partitions, verify
    SshHiveAction(master, List(
      s"alter table $testDbName.$testTableName add partition (month=1, day=1)",
      s"alter table $testDbName.$testTableName add partition (month=1, day=2)",
      s"alter table $testDbName.$testTableName add partition (month=2, day=1)",
      s"alter table $testDbName.$testTableName add partition (month=2, day=2)"))
    val resultPartitions = SshHiveAction(master, List(s"show partitions $testDbName.$testTableName"))
    val partitions = resultPartitions.split("\n")
    assert(partitions.length == 4)
    assert(partitions.contains("month=1/day=1"))
    assert(partitions.contains("month=1/day=2"))
    assert(partitions.contains("month=2/day=1"))
    assert(partitions.contains("month=2/day=2"))

    //load data into the partitions (2 rows per partition)
    SshMultiAction(master, List(
      s"echo a | tee --append $testFileName",
      s"echo b | tee --append $testFileName"))
    SshHiveAction(master, List(
      s"load data local inpath '$testFileName' into table $testDbName.$testTableName partition (month=1, day=1)",
      s"load data local inpath '$testFileName' into table $testDbName.$testTableName partition (month=1, day=2)",
      s"load data local inpath '$testFileName' into table $testDbName.$testTableName partition (month=2, day=1)",
      s"load data local inpath '$testFileName' into table $testDbName.$testTableName partition (month=2, day=2)"))

    //Run Hive Query, verify count is correct
    val results = SshHiveAction(master, List(s"select count(*) from $testDbName.$testTableName"))
    assertResult("8") (results.stripLineEnd)
  }

  test("Copy data to another cluster, Run Tests, Destroy It") {

    //create a 'target' docker cluster
    val newCluster = CreateCompleteClusterCliAction(List("2"), conf)
    assertResult(AwsRunning) (newCluster.master.status)
    val newClusterId = newCluster.master.id

    var clusters = ListAwsCliAction(List(), conf)
    val sourceCluster = getCluster(clusterId, clusters)

    //create the source configuration to specify what to copy from the original cluster
    val oldSourceConf = conf +
      ("source.address" -> sourceCluster.master.publicIp) +
      ("source.tables" -> s"$testDbName.$testTableName") +
      ("source.key.file" -> AwsUtilities.getAwsProp(conf, GeneralConstants.keyFile)) +
      ("source.user" -> AwsUtilities.getAwsProp(conf, GeneralConstants.user)) +
      ("default.partition.count" -> "2")

    //copy the data to the 'target' cluster
    CopyAwsCliAction(List(newClusterId), oldSourceConf)

    //verify.  As we only copied 2 partitions out of 4, it should be half the data
    val awsNode = NodeFactory.getAwsNode(conf, newCluster.master)
    val results = SshHiveAction(awsNode, List(s"select count(*) from $testDbName.$testTableName"))
    assertResult("4") (results.stripLineEnd)

    //try copy without source tables or files.  Should fail
    val invalidNewSourceConf = oldSourceConf - "source.tables"
    assertThrows[IllegalArgumentException] (CopyAwsCliAction(List(clusterId), invalidNewSourceConf),
      "Exception expected as source.tables and source.files is not defined.")

    //Destroy the 'target' docker cluster
    DestroyAwsCliAction(List(newClusterId), conf)
    clusters = ListAwsCliAction(List(), conf)
    assertNoCluster(newClusterId, clusters)
  }

  test("Stop and restart cluster") {

    //Stop docker cluster and verify
    StopAwsCliAction(List(clusterId), conf)
    var clusterMetas = ListAwsCliAction(List(), conf)
    var clusterMeta = getCluster(clusterId, clusterMetas)
    assertResult(AwsSuspended) (clusterMeta.master.status)

    //try to extend, should not be possible to do.
    assertThrows[IllegalArgumentException] (ExtendAwsCliAction(List(clusterId), conf),
      "Exception expected as cannot extend a stopped node.")

    //Restart aws cluster and verify
    StartAwsCliAction(List(clusterId), conf)
    clusterMetas = ListAwsCliAction(List(), conf)
    clusterMeta = getCluster(clusterId, clusterMetas)
    assertResult(AwsRunning) (clusterMeta.master.status)

    //Run the same query as in the last test.  Data should still be in the cluster.
    val awsNode = NodeFactory.getAwsNode(conf, clusterMeta.master)
    val results = SshHiveAction(awsNode, List(s"select count(*) from $testDbName.$testTableName"))
    assertResult("8") (results.stripLineEnd)
  }


  test("Destroy the local cluster") {
    //Destroy the local cluster and verify
    DestroyAwsCliAction(List(clusterId), conf)
    val clusters = ListAwsCliAction(List(), conf)
    assertNoCluster(clusterId, clusters)
  }

  def getCluster(clusterId: String, clusters: List[AwsCluster]) : AwsCluster = {
    val results = clusters.filter(_.master.id.equals(clusterId))
    if (results.length > 2) {
      fail("Invalid state, more than one cluster returned.")
    }

    if (results.length == 0) {
      fail(s"AWS cluster $clusterId not found")
    }
    results.last
  }

  def assertNoCluster(clusterId: String, clusters: List[AwsCluster]) = {
    clusters.foreach(c => {
      if (c.master.id.equals(clusterId)) {
        assert(!c.master.status.equals(AwsRunning))
      }
    })
  }
}
