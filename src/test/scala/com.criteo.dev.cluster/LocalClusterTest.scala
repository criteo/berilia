package com.criteo.dev.cluster

import com.criteo.dev.cluster.docker._
import com.criteo.dev.cluster.utils.test.LoadConfig
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  * This class spawns some local dev cluster and runs all the operations on them.
  */
class LocalClusterTest extends FunSuite with BeforeAndAfter with LoadConfig {

  def testDbName = "testdb"
  def testTableName = "testtable"
  def testFileName = "testfile"

  def testFileName2 = "testfile2"
  def testTableName2 = "testtable2"
  def testTableName3 = "testtable3"
  def testTableName4 = "testtable4"

  var dockerId: String = null

  before {
  }

  test("Create a cluster, list it") {

    //Create a docker cluster
    val dockerMeta = CreateLocalCliAction(List(), config)
    assertResult(DockerRunning) (dockerMeta.dockerState)
    dockerId = dockerMeta.id

    //List and make sure it is returned
    val dockerMetas = ListDockerCliAction(List(), config)
    getDockerMeta(dockerId, dockerMetas)
  }

  test("Create test database, table, partitions in Node, Run a query") {

    //List and get the created docker cluster
    val dockerMetas = ListDockerCliAction(List(), config)
    val dockerMeta = getDockerMeta(dockerId, dockerMetas)

    //Run some Hive commands on the docker cluster.
    val dockerNode = NodeFactory.getDockerNode(conf, dockerMeta)

    //create database, verify
    SshHiveAction(dockerNode, List(s"create database $testDbName"))
    val result = SshHiveAction(dockerNode, List("show databases"))
    assert(result.contains(testDbName))

    //create table, verify
    SshHiveAction(dockerNode, List(s"create table $testDbName.$testTableName (name string) partitioned by (month int, day int)"))
    val resultTables = SshHiveAction(dockerNode, List(
      s"use $testDbName",
      s"show tables"))
    assert(resultTables.contains(testTableName))

    //create 4 partitions, verify
    SshHiveAction(dockerNode, List(
      s"alter table $testDbName.$testTableName add partition (month=1, day=1)",
      s"alter table $testDbName.$testTableName add partition (month=1, day=2)",
      s"alter table $testDbName.$testTableName add partition (month=2, day=1)",
      s"alter table $testDbName.$testTableName add partition (month=2, day=2)"))
    val resultPartitions = SshHiveAction(dockerNode, List(s"show partitions $testDbName.$testTableName"))
    val partitions = resultPartitions.split("\n")
    assert(partitions.length == 4)
    assert(partitions.contains("month=1/day=1"))
    assert(partitions.contains("month=1/day=2"))
    assert(partitions.contains("month=2/day=1"))
    assert(partitions.contains("month=2/day=2"))

    //load data into the partitions (2 rows per partition)
    SshMultiAction(dockerNode, List(
      s"echo a | tee --append $testFileName",
      s"echo b | tee --append $testFileName"))
    SshHiveAction(dockerNode, List(
      s"load data local inpath '$testFileName' into table $testDbName.$testTableName partition (month=1, day=1)",
      s"load data local inpath '$testFileName' into table $testDbName.$testTableName partition (month=1, day=2)",
      s"load data local inpath '$testFileName' into table $testDbName.$testTableName partition (month=2, day=1)",
      s"load data local inpath '$testFileName' into table $testDbName.$testTableName partition (month=2, day=2)"))

    //Run Hive Query, verify count is correct
    val results = SshHiveAction(dockerNode, List(s"select count(*) from $testDbName.$testTableName"))
    assertResult("8") (results.stripLineEnd)
  }


  test("Stop and restart cluster") {

    //Stop docker cluster and verify
    StopLocalCliAction(List(dockerId), config)
    var dockerMetas = ListDockerCliAction(List(), config)
    var dockerMeta = getDockerMeta(dockerId, dockerMetas)
    assertResult(DockerStopped) (dockerMeta.dockerState)


    //Restart docker cluster and verify
    StartLocalCliAction(List(dockerId), config)
    dockerMetas = ListDockerCliAction(List(), config)
    dockerMeta = getDockerMeta(dockerId, dockerMetas)
    assertResult(DockerRunning) (dockerMeta.dockerState)

    //Run the same query as in the last test.  Data should still be in the cluster.
    val dockerNode = NodeFactory.getDockerNode(conf, dockerMeta)
    val results = SshHiveAction(dockerNode, List(s"select count(*) from $testDbName.$testTableName"))
    assertResult("8") (results.stripLineEnd)
  }


  test("Copy data to another cluster, Run Tests, Destroy It") {

    //create a 'target' docker cluster
    val newDockerMeta = CreateLocalCliAction(List(), config)
    assertResult(DockerRunning) (newDockerMeta.dockerState)
    val newDockerId = newDockerMeta.id

    //create the source configuration to specify what to copy from the original cluster
    val newSourceConf = conf + ("source.user" -> conf.get("target.local.cluster.user").get) +
      ("source.address" -> DockerUtilities.getSshHost(conf)) +
      ("source.port" -> DockerUtilities.getSshPort(dockerId)) +
      ("source.tables" -> s"$testDbName.$testTableName") +
      ("source.key.file" -> DockerConstants.dockerPrivateKey)
      ("default.partition.count" -> "2")

    //copy the data to the 'target' cluster
    CopyLocalCliAction(List(newDockerId), config.copy(backCompat = newSourceConf))

    //verify.  As we only copied 2 partitions out of 4, it should be half the data
    val dockerNode = NodeFactory.getDockerNode(conf, newDockerMeta)
    val results = SshHiveAction(dockerNode, List(s"select count(*) from $testDbName.$testTableName"))
    assertResult("4") (results.stripLineEnd)

    //try copy without source tables or files.  Should fail
    val invalidNewSourceConf = newSourceConf - "source.tables"
    assertThrows[IllegalArgumentException] (CopyLocalCliAction(List(newDockerId), config.copy(backCompat = invalidNewSourceConf)),
      "Exception expected as source.tables and source.files is not defined.")

    //Destroy the 'target' docker cluster
    DestroyLocalCliAction(List(newDockerId), config)
  }

  //There is a very miniscule chance (0.00.....001)^100 that this test does not generate data and fails.
  //Sorry about that, it comes with sampling probability.
  test("Test sampling.") {

    val dockerMetas = ListDockerCliAction(List(), config)
    val dockerMeta = getDockerMeta(dockerId, dockerMetas)
    val dockerNode = NodeFactory.getDockerNode(conf, dockerMeta)

      val newDockerMeta = CreateLocalCliAction(List(), config)

    val newDockerNode = NodeFactory.getDockerNode(conf, newDockerMeta)

    //create partition table with 4 partitions, and two unpartitioned tables, to test both case.
    SshHiveAction(dockerNode, List(s"create database if not exists dev_cluster_sample",
      s"create database if not exists $testDbName",
      s"create table $testDbName.$testTableName2 (name string) partitioned by (month int, day int)",
      s"alter table $testDbName.$testTableName2 add partition (month=1, day=1)",
      s"alter table $testDbName.$testTableName2 add partition (month=1, day=2)",
      s"alter table $testDbName.$testTableName2 add partition (month=2, day=1)",
      s"alter table $testDbName.$testTableName2 add partition (month=2, day=2)",
      s"create table $testDbName.$testTableName3 (name string)",
      s"create table $testDbName.$testTableName4 (name string)"))

    //load data into the partitions (more rows to increase chance that sampling generates some data)
    SshMultiAction(dockerNode,
      (1 to 100).map(i => s"echo $i | tee --append $testFileName2").toList)


    SshHiveAction(dockerNode, List(
      s"load data local inpath '$testFileName2' into table $testDbName.$testTableName2 partition (month=1, day=1)",
      s"load data local inpath '$testFileName2' into table $testDbName.$testTableName2 partition (month=1, day=2)",
      s"load data local inpath '$testFileName2' into table $testDbName.$testTableName2 partition (month=2, day=1)",
      s"load data local inpath '$testFileName2' into table $testDbName.$testTableName2 partition (month=2, day=2)",
      s"load data local inpath '$testFileName2' into table $testDbName.$testTableName3",
      s"load data local inpath '$testFileName2' into table $testDbName.$testTableName4"))

    //create the source configuration to specify sample for some tables, and not for other tables,
    //to test both cases
    val newSourceConf = conf + ("source.user" -> conf.get("target.local.cluster.user").get) +
      ("source.address" -> DockerUtilities.getSshHost(conf)) +
      ("source.port" -> DockerUtilities.getSshPort(dockerId)) +
      ("source.tables" -> s"$testDbName.$testTableName2, $testDbName.$testTableName3, $testDbName.$testTableName4") +
      ("source.key.file" -> DockerConstants.dockerPrivateKey) +
      ("default.partition.count" -> "3") +
      ("source.copy.sample.threshold" -> "1") +
      (s"$testDbName.$testTableName2.sample.prob" -> "0.99999999999") +
      (s"$testDbName.$testTableName3.sample.prob" -> "0.9999999999999999") +
      (s"$testDbName.$testTableName4.sample.prob" -> "1.0")

    CopyLocalCliAction(List(newDockerMeta.id), config.copy(backCompat = newSourceConf))

    //Run Hive Query, verify count is correct
    var results = SshHiveAction(newDockerNode, List(s"select count(*) from $testDbName.$testTableName2"))
    assert(results.stripLineEnd.toInt > 0)

    results = SshHiveAction(newDockerNode, List(s"select count(*) from $testDbName.$testTableName3"))
    assert(results.stripLineEnd.toInt > 0)

    results = SshHiveAction(newDockerNode, List(s"select count(*) from $testDbName.$testTableName4"))
    assert(results.stripLineEnd.toInt > 0)

    DestroyLocalCliAction(List(newDockerMeta.id), config)
  }


  test("Destroy the local cluster") {
    //Destroy the local cluster and verify
    DestroyLocalCliAction(List(dockerId), config)
    val dockerMetas = ListDockerCliAction(List(), config)
    assertNoDocker(dockerId, dockerMetas)
  }


  def getDockerMeta(dockerId: String, dockerMetas: Array[DockerMeta]) : DockerMeta = {
    val results = dockerMetas.filter(_.id.equals(dockerId))
    if (results.length > 2) {
      fail("Invalid state, more than one docker meta returned.")
    }

    if (results.length == 0) {
      fail(s"Docker $dockerId not found")
    }
    results.last
  }

  def assertNoDocker(dockerId: String, dockerMetas: Array[DockerMeta]) = {
    dockerMetas.foreach(u => assert(!u.id.equals(dockerId)))
  }
}