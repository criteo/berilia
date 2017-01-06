package com.criteo.dev.cluster

import com.criteo.dev.cluster.aws._
import com.criteo.dev.cluster.docker.{DestroyGatewayCliAction, CreateGatewayCliAction, DockerMeta, ListDockerCliAction}
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  * Test advance features of AWS cluster.
  *
  * This will only run if ./conf/target-aws.xml is configured with a valid AWS account.
  *
  * So it is not enabled in the actual suite.
  */
class AwsClusterAdvanceTest extends FunSuite with BeforeAndAfter {

  val conf = ConfigManager.load(List())

  def testDbName = "testdb"
  def testTableName = "testtable"
  def testFileName = "testfile"
  def currentUser = System.getenv("USER")

  var clusterId: String = null

  test("Create a cluster, populate cluster") {

    //Create a docker cluster
    val cluster = CreateAwsCliAction(List("3"), conf)
    clusterId = cluster.master.id

    assertResult(2)(cluster.slaves.size)
    assertResult(AwsRunning)(cluster.master.status)
    assertResult(currentUser)(cluster.user)

    //create database, table, and 2 partitions.
    val master = NodeFactory.getAwsNode(conf, cluster.master)
    SshHiveAction(master, List(s"create database $testDbName"))
    SshHiveAction(master, List(s"create table $testDbName.$testTableName (name string) partitioned by (month int)"))
    SshHiveAction(master, List(
      s"alter table $testDbName.$testTableName add partition (month=1)",
      s"alter table $testDbName.$testTableName add partition (month=1)"))

    //load data into the partitions (2 rows per partition)
    SshMultiAction(master, List(
      s"echo a | tee --append $testFileName",
      s"echo b | tee --append $testFileName"))
    SshHiveAction(master, List(
      s"load data local inpath '$testFileName' into table $testDbName.$testTableName partition (month=1)",
      s"load data local inpath '$testFileName' into table $testDbName.$testTableName partition (month=2)"))
  }


  test("Reconfigure a cluster and test the query.") {
    var clusters = ListAwsCliAction(List(), conf)
    val cluster = getCluster(clusterId, clusters)
    val master = NodeFactory.getAwsNode(conf, cluster.master)
    ConfigureAwsCliAction(List(clusterId), conf)
    RestartServicesCliAction(List(clusterId), conf)

    //Run Hive Query, verify count is correct
    val results = SshHiveAction(master, List(s"select count(*) from $testDbName.$testTableName"))
    assertResult("4") (results.stripLineEnd)
  }

  test("Create a gateway") {
    CreateGatewayCliAction(List(clusterId), conf)
    //TODO- Docker Gateway is in interactive mode as part of the CLI, so doesn't actually run as part of the program.
    //No way to unit test for now.. should we add a background mode like local-cluster?
    DestroyGatewayCliAction(List(), conf)
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

}
