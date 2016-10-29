package com.criteo.dev.cluster

import com.criteo.dev.cluster.docker._
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  * This class spawns some local dev cluster and runs all the operations on them.
  */
class LocalClusterTest extends FunSuite with BeforeAndAfter {

  val conf = ConfigManager.load(List())

  def testDbName = "testdb"
  def testTableName = "testtable"
  def testFileName = "testfile"

  var dockerId: String = null

  before {
  }

  test("Create a cluster, list it") {

    //Create a docker cluster
    val dockerMeta = CreateDockerCliAction(List(), conf)
    assertResult(DockerRunning) (dockerMeta.dockerState)
    dockerId = dockerMeta.id

    //List and make sure it is returned
    val dockerMetas = ListDockerCliAction(List(), conf)
    getDockerMeta(dockerId, dockerMetas)
  }

  test("Create test database, table, partitions in Node, Run a query") {

    //List and get the created docker cluster
    val dockerMetas = ListDockerCliAction(List(), conf)
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
    StopDockerCliAction(List(dockerId), conf)
    var dockerMetas = ListDockerCliAction(List(), conf)
    var dockerMeta = getDockerMeta(dockerId, dockerMetas)
    assertResult(DockerStopped) (dockerMeta.dockerState)


    //Restart docker cluster and verify
    StartDockerCliAction(List(dockerId), conf)
    dockerMetas = ListDockerCliAction(List(), conf)
    dockerMeta = getDockerMeta(dockerId, dockerMetas)
    assertResult(DockerRunning) (dockerMeta.dockerState)

    //Run the same query as in the last test.  Data should still be in the cluster.
    val dockerNode = NodeFactory.getDockerNode(conf, dockerMeta)
    val results = SshHiveAction(dockerNode, List(s"select count(*) from $testDbName.$testTableName"))
    assertResult("8") (results.stripLineEnd)
  }


  test("Copy data to another cluster, Run Tests, Destroy It") {

    //create a 'target' docker cluster
    val newDockerMeta = CreateDockerCliAction(List(), conf)
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
    DockerCopyCliAction(List(newDockerId), newSourceConf)

    //verify.  As we only copied 2 partitions out of 4, it should be half the data
    val dockerNode = NodeFactory.getDockerNode(conf, newDockerMeta)
    val results = SshHiveAction(dockerNode, List(s"select count(*) from $testDbName.$testTableName"))
    assertResult("4") (results.stripLineEnd)

    //try copy without source tables or files.  Should fail
    val invalidNewSourceConf = newSourceConf - "source.tables"
    assertThrows[IllegalArgumentException] (DockerCopyCliAction(List(newDockerId), invalidNewSourceConf),
      "Exception expected as source.tables and source.files is not defined.")

    //Destroy the 'target' docker cluster
    DestroyDockerCliAction(List(newDockerId), conf)
  }

  test("Destroy the local cluster") {
    //Destroy the local cluster and verify
    DestroyDockerCliAction(List(dockerId), conf)
    val dockerMetas = ListDockerCliAction(List(), conf)
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