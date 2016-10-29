package com.criteo.dev.cluster.aws

import java.util.{Collections, Properties}

import com.criteo.dev.cluster.{GeneralConstants, GeneralUtilities}
import org.jclouds.aws.ec2.compute.AWSEC2TemplateOptions
import org.jclouds.compute.domain.NodeMetadata
import org.joda.time.DateTime
import org.scala_tools.time.Imports._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Creates an AWS cluster.
  */
object CreateClusterAction {

  private val logger = LoggerFactory.getLogger(CreateClusterAction.getClass)


  def apply(conf: Map[String, String], nodes: Int, masterImage: String, slaveImage: String): JcloudCluster = {
    logger.info(s"Creating $nodes node(s) in parallel.")
    val createMaster = GeneralUtilities.getFuture {
      val master: NodeMetadata = AwsUtilities.retryAwsAction(new RetryableCreate(conf, masterImage))
      logger.info(s"Successfully created master ${master.getId}")
      master
    }

    val createSlaves = (1 to (nodes - 1)).map(i => GeneralUtilities.getFuture {
      val slave: NodeMetadata = AwsUtilities.retryAwsAction(new RetryableCreate(conf, slaveImage))
      logger.info(s"Successfully created slave ${slave.getId}")
      slave
    })

    val master = Await.result(createMaster, Duration.Inf)
    val slaves = createSlaves.map(sf => Await.result(sf, Duration.Inf))

    val cluster = new JcloudCluster(master, collection.mutable.Set(slaves.toArray:_*))
    tagCluster(conf, cluster)
    cluster
  }

  /**
    * Assign host names to cluster, tag them with name.
    *
    * Also tag with cluster tag for identification as a cluster unit.
    */
  def tagCluster(conf: Map[String, String], cluster: JcloudCluster) = {
    val region = AwsUtilities.getAwsProp(conf, AwsConstants.region)

    logger.info("Assigning node names and tagging instances.")

    val tagApi = AwsUtilities.getEc2Api(conf).getTagApiForRegion(region).get()

    //tag the master
    val masterMeta = cluster.master.getUserMetadata
    masterMeta.put(AwsConstants.hostName, GeneralConstants.masterHostName)
    val masterId = AwsUtilities.stripRegion(conf, cluster.master.getId)
    tagApi.applyToResources(masterMeta, List(masterId).asJava)
    logger.info(s"Tagged instance $masterId, hostname=${GeneralConstants.masterHostName}")

    //tag the slaves
    val tagFutures = cluster.slaves.zipWithIndex.foreach {
      case (sm, i) => {
        val hostname = s"dev-slave-$i"
        val metadata = sm.getUserMetadata
        metadata.put(AwsConstants.master, AwsUtilities.stripRegion(conf, cluster.master.getId))
        metadata.put(AwsConstants.hostName, hostname)
        val id = AwsUtilities.stripRegion(conf, sm.getId)
        tagApi.applyToResources(metadata, List(id).asJava)
        logger.info(s"Tagged instance $id, hostname=$hostname")
      }
    }
  }
}

class RetryableCreate(conf: Map[String, String], imageId: String) extends Retryable[NodeMetadata] {

  def action() : NodeMetadata = {

    //set some AWS properties
    val keyPair = AwsUtilities.getAwsProp(conf, AwsConstants.keyPair)
    val region = AwsUtilities.getAwsProp(conf, AwsConstants.region)
    val instanceType = AwsUtilities.getAwsProp(conf, AwsConstants.instanceType)
    val securityGroup = AwsUtilities.getAwsProp(conf, AwsConstants.securityGroup)
    val subnet = AwsUtilities.getAwsProp(conf, AwsConstants.subnet)

    val mComputeService = AwsUtilities.getComputeService(conf)

    val expireTime = DateTime.now(DateTimeZone.UTC) + AwsConstants.extensionTime
    val expireTimeString = AwsUtilities.dtToString(expireTime)

    val template = mComputeService.templateBuilder.hardwareId(instanceType).
      imageId(s"$region/$imageId").build
    template.getOptions.as(classOf[AWSEC2TemplateOptions])
      .keyPair(keyPair)
      .securityGroupIds(securityGroup)
      .subnetId(subnet)
      //.blockUntilRunning(true)
      .blockOnPort(22, 120)
      .tags(Collections.singletonList(AwsConstants.groupTag))
      .userMetadata(AwsConstants.userTagKey, System.getenv("USER"))
      .userMetadata(AwsConstants.expireTime, expireTimeString)
      .userMetadata(AwsConstants.createTime, AwsUtilities.getCurrentTime())


    val result: java.util.Set[_ <: NodeMetadata] = mComputeService.createNodesInGroup(
      AwsConstants.groupTag, 1, template)

    require(result.size() == 1)
    result.iterator().next()
  }
}