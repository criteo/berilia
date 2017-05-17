package com.criteo.dev.cluster

import com.criteo.dev.cluster.docker._
import com.criteo.dev.cluster.s3._
import com.criteo.dev.cluster.utils.test.LoadConfig
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  * This will only run if ./conf/target-aws.xml is configured with a valid AWS account.
  *
  * So it is not enabled in the actual suite.
  */
class S3Test extends FunSuite with BeforeAndAfter with LoadConfig {

  val currentUser = System.getenv("USER")
  val tempBucketHint = "temp-bucket"
  val testBucketHint = "test-bucket"
  val testBucketName = GeneralUtilities.getConfStrict(conf, BucketConstants.bucketPrefix, "") + s"-$testBucketHint"
  val tempBucketName = GeneralUtilities.getConfStrict(conf, BucketConstants.bucketPrefix, "") + s"-$tempBucketHint"

  def testDbName = "testdb"
  def testTableName = "testtable"
  def testFileName = "testfile"
  def testDirName = "testDir"

  test("Create some buckets, list them, leave one") {
    DestroyBucketCliAction(List(testBucketName), config)
    DestroyBucketCliAction(List(tempBucketName), config)

    val randomBucketMeta = CreateBucketCliAction(List(), config)
    val tempBucketMeta = CreateBucketCliAction(List(tempBucketHint), config)
    val testBucketMeta = CreateBucketCliAction(List(testBucketHint), config)
    assertResult(tempBucketName) (tempBucketMeta.name)
    assertResult(testBucketName) (testBucketMeta.name)

    val bucketMetas = ListBucketCliAction(List(), config)
    getBucket(tempBucketMeta.name, bucketMetas)
    getBucket(testBucketMeta.name, bucketMetas)
    getBucket(randomBucketMeta.name, bucketMetas)

    DestroyBucketCliAction(List(tempBucketName), config)
    DestroyBucketCliAction(List(randomBucketMeta.name), config)
    val newBucketMetas = ListBucketCliAction(List(), config)
    getBucket(testBucketName, newBucketMetas)
    assertNoBucket(tempBucketName, newBucketMetas)
    assertNoBucket(randomBucketMeta.name, newBucketMetas)
  }


  test("Copy some data into the bucket") {
    //Create a docker cluster
    val dockerMeta = CreateLocalCliAction(List(), config)

    val dockerNode = NodeFactory.getDockerNode(conf, dockerMeta)

    //create database, verify
    SshHiveAction(dockerNode, List(s"create database $testDbName"))

    //create table, verify
    SshHiveAction(dockerNode, List(s"create table $testDbName.$testTableName (name string) partitioned by (month int, day int)"))

    //create 4 partitions, verify
    SshHiveAction(dockerNode, List(
      s"alter table $testDbName.$testTableName add partition (month=1, day=1)",
      s"alter table $testDbName.$testTableName add partition (month=1, day=2)",
      s"alter table $testDbName.$testTableName add partition (month=2, day=1)",
      s"alter table $testDbName.$testTableName add partition (month=2, day=2)"))

    //load data into the partitions (2 rows per partition)
    SshMultiAction(dockerNode, List(
      s"echo a | tee --append $testFileName",
      s"echo b | tee --append $testFileName"))
    SshHiveAction(dockerNode, List(
      s"load data local inpath '$testFileName' into table $testDbName.$testTableName partition (month=1, day=1)",
      s"load data local inpath '$testFileName' into table $testDbName.$testTableName partition (month=1, day=2)",
      s"load data local inpath '$testFileName' into table $testDbName.$testTableName partition (month=2, day=1)",
      s"load data local inpath '$testFileName' into table $testDbName.$testTableName partition (month=2, day=2)",
      s"load data local inpath '$testFileName' into table $testDbName.$testTableName partition (month=3, day=1)",
      s"load data local inpath '$testFileName' into table $testDbName.$testTableName partition (month=3, day=2)"))

    //Put the data into HDFS too (to test HDFS file copy)
    SshMultiAction(dockerNode, List(
      s"hdfs dfs -mkdir /tmp/$testDirName",
      s"hdfs dfs -put $testFileName /tmp/$testDirName"))

    val copyTableConf = conf + ("source.user" -> conf.get("target.local.cluster.user").get) +
      ("source.address" -> DockerUtilities.getSshHost(conf)) +
      ("source.port" -> DockerUtilities.getSshPort(dockerMeta.id)) +
      ("source.tables" -> s"$testDbName.$testTableName") +
      ("source.key.file" -> DockerConstants.dockerPrivateKey)
    ("distcp.top.partition.count" -> "2")
    CopyBucketCliAction(List(testBucketName), config.copy(backCompat = copyTableConf))

    val copyFileConf = conf + ("source.user" -> conf.get("target.local.cluster.user").get) +
      ("source.address" -> DockerUtilities.getSshHost(conf)) +
      ("source.port" -> DockerUtilities.getSshPort(dockerMeta.id)) +
      ("source.files" -> s"/tmp/$testDirName/$testFileName") +
      ("source.key.file" -> DockerConstants.dockerPrivateKey)
    CopyBucketCliAction(List(testBucketName), config.copy(backCompat = copyFileConf))

    //this should have copied 4 partitions over (2 top-level ones)
    val bucketMeta = DescribeBucketCliAction(List(testBucketName), config)
    assertResult(2)(bucketMeta.dataBlocks.size)
    assertResult(DataType.hive)(bucketMeta.dataBlocks(0).dataType)
    assertResult(currentUser)(bucketMeta.dataBlocks(0).writer)

    assertResult(5)(bucketMeta.dataBlocks(0).filteredListing.size)
    assertResult(s"alter table $testTableName add if not exists partition (month='2', day='1')")(bucketMeta.dataBlocks(0).filteredListing(1).trim)
    assertResult(s"alter table $testTableName add if not exists partition (month='2', day='2')")(bucketMeta.dataBlocks(0).filteredListing(2).trim)
    assertResult(s"alter table $testTableName add if not exists partition (month='3', day='1')")(bucketMeta.dataBlocks(0).filteredListing(3).trim)
    assertResult(s"alter table $testTableName add if not exists partition (month='3', day='2')")(bucketMeta.dataBlocks(0).filteredListing(4).trim)

    assertResult(DataType.hdfs)(bucketMeta.dataBlocks(1).dataType)
    assertResult(currentUser)(bucketMeta.dataBlocks(1).writer)

    assertResult(1)(bucketMeta.dataBlocks(1).filteredListing.size)
    assertResult(s"s3a://$testBucketName/tmp/$testDirName/$testFileName")(bucketMeta.dataBlocks(1).filteredListing(0).trim)
  }


  test("Create a new docker cluster and point data to it.") {
    val dockerMeta = CreateLocalCliAction(List(), config)
    val dockerNode = NodeFactory.getDockerNode(conf, dockerMeta)
    AttachBucketLocalCliAction(List(testBucketName, dockerMeta.id), config)
    val resultPartitions = SshHiveAction(dockerNode, List(s"show partitions $testDbName.$testTableName"))
    val partitions = resultPartitions.split("\n")
    assert(partitions.length == 4)
    assert(partitions.contains("month=2/day=1"))
    assert(partitions.contains("month=2/day=2"))
    assert(partitions.contains("month=3/day=1"))
    assert(partitions.contains("month=3/day=2"))

    //Run Hive Query, verify count is correct
    val countResults = SshHiveAction(dockerNode, List(s"select count(*) from $testDbName.$testTableName"))
    assertResult("8") (countResults.stripLineEnd)

    //Cat HDFS file, verify result is correct
    val hdfsResults = SshAction(dockerNode, s"hdfs dfs -cat s3a://$testBucketName/tmp/$testDirName/$testFileName", returnResult=true)
    val hdfsLine = hdfsResults.split("\n")
    assert(hdfsLine.length == 2)
    assert(hdfsLine.contains("a"))
    assert(hdfsLine.contains("b"))

    DestroyLocalCliAction(List(dockerMeta.id), config)
    DestroyBucketCliAction(List(testBucketName), config)
  }

  def getBucket(bucketId: String, bucketMetas: Set[BucketMeta]) = {
    val results = bucketMetas.filter(b => b.name.equals(bucketId))
    if (results.size > 2) {
      fail("Invalid state, more than one bucket returned.")
    }

    if (results.size == 0) {
      fail(s"Bucket $bucketId not found")
    }
    results.last
  }

  def assertNoBucket(bucketId: String, bucketMetas: Set[BucketMeta]) = {
    bucketMetas.foreach(b => assert(!b.name.equals(bucketId)))
  }
}
