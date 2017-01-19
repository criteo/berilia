package com.criteo.dev.cluster.aws

import java.util
import java.util.Properties

import com.criteo.dev.cluster.aws.AwsUtilities.NodeRole.NodeRole
import com.criteo.dev.cluster.s3.BucketUtilities
import com.criteo.dev.cluster.{GeneralConstants, GeneralUtilities}
import com.google.common.base.Predicate
import com.google.common.collect.ImmutableSetMultimap
import org.jclouds.aws.ec2.AWSEC2Api
import org.jclouds.compute.config.ComputeServiceProperties
import org.jclouds.compute.domain.NodeMetadata.Status
import org.jclouds.compute.domain.{ComputeMetadata, NodeMetadata}
import org.jclouds.compute.{ComputeService, ComputeServiceContext}
import org.jclouds.ec2.EC2Api
import org.jclouds.ec2.domain.Image
import org.jclouds.ec2.features.TagApi
import org.jclouds.{Constants, ContextBuilder}
import org.joda.time.DateTime
import org.scala_tools.time.Imports._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.util.Random

/**
  * Generic utilities for accessing AWS nodes.
  */
object AwsUtilities {

  private val logger = LoggerFactory.getLogger(AwsUtilities.getClass)

  def getAwsProp(conf: Map[String, String], propKey: String) : String = {
    GeneralUtilities.getConfStrict(conf, s"${AwsConstants.awsKeyPrefix}.$propKey", GeneralConstants.targetAwsProps)
  }

  //--- AWS API---

  def getComputeService(conf: Map[String, String]) : ComputeService = {

    val keyid = getAwsProp(conf, AwsConstants.accessId)
    val key = getAwsProp(conf, AwsConstants.accessKey)
    val region = getAwsProp(conf, AwsConstants.region)
    val overrides = getEc2Overrides(region)

    logger.info(s"Connecting to AWS in region ${region}")

    //create the node using jcloud
    val mComputeServiceContext = ContextBuilder
      .newBuilder("aws-ec2")
      .credentials(keyid, key)
        .endpoint(s"https://ec2.${region}.amazonaws.com")
      .overrides(overrides)
      .buildView(classOf[ComputeServiceContext])
    mComputeServiceContext.getComputeService()
  }

  def getEc2Api(conf: Map[String, String]) : AWSEC2Api = {

    val keyid = getAwsProp(conf, AwsConstants.accessId)
    val key = getAwsProp(conf, AwsConstants.accessKey)
    val region = getAwsProp(conf, AwsConstants.region)
    val overrides = getEc2Overrides(region)

    logger.info(s"Connecting to AWS in region ${region}")

    ContextBuilder
      .newBuilder("aws-ec2")
      .credentials(keyid, key)
      .overrides(overrides)
      .buildApi(classOf[AWSEC2Api])
  }

  def getEc2Overrides (region: String) : Properties = {
    //set some jcloud properties
    val overrides = new Properties
    overrides.put(ComputeServiceProperties.POLL_INITIAL_PERIOD, String.valueOf(60L * 1000L))
    overrides.put(ComputeServiceProperties.POLL_MAX_PERIOD, String.valueOf(600L * 1000L))
    overrides.put(Constants.PROPERTY_MAX_RETRIES, String.valueOf(60))
    overrides.put("jclouds.regions", region)
    overrides
  }

  //Queries for AWS Nodes

  val predicate = {
    new Predicate[ComputeMetadata]() {
      def apply(computeMetadata: ComputeMetadata): Boolean = {
        System.getenv("USER").equals(getUser(computeMetadata)) &&
          computeMetadata.getTags().contains(AwsConstants.groupTag)
      }
    }
  }

  val allUserPredicate = {
    new Predicate[ComputeMetadata]() {
      def apply(computeMetadata: ComputeMetadata): Boolean = {
        computeMetadata.getTags().contains(AwsConstants.groupTag)
      }
    }
  }

  def getUserClusters(conf: Map[String, String]) : Iterable[JcloudCluster] = {
    val computeService = getComputeService(conf)
    //in theory, jclouds should retry this as it is a GET request, but we can use the 'retryAwsAction'
    //if this does not prove the case
    val javaSet = computeService.listNodesDetailsMatching(predicate)
    getClustersFromNodeMetas(conf, javaSet.toSet)
  }

  def getUserCluster(conf: Map[String, String], id: String) : JcloudCluster = {
    val clusters = getUserClusters(conf)
    val matches = clusters.filter(_.master.getId().equals(AwsUtilities.getFullId(conf, id)))
    require(matches.size == 1, s"No instances found matching $id")
    val result = matches.last
    result
  }

  def getUser(computeMetadata: ComputeMetadata) : String = {
    computeMetadata.getUserMetadata().get(AwsConstants.userTagKey)
  }

  def getCluster(conf: Map[String, String], instanceId: String) : JcloudCluster = {
    val computeService = getComputeService(conf)
    //in theory, jclouds should retry this as it is a GET request, but we can use the 'retryAwsAction'
    //if this does not prove the case
    val javaSet = computeService.listNodesDetailsMatching(allUserPredicate)
    val clusters = getClustersFromNodeMetas(conf, javaSet.toSet)
    val matches = clusters.filter(_.master.getId().equals(AwsUtilities.getFullId(conf, instanceId)))
    require(matches.size == 1, s"No instances found matching $instanceId")
    val result = matches.last
    result
  }


  def getAllClusters(conf: Map[String, String]) : Iterable[JcloudCluster] = {
    val computeService = getComputeService(conf)
    //in theory, jclouds should retry this as it is a GET request, but we can use the 'retryAwsAction'
    //if this does not prove the case
    val javaSet = computeService.listNodesDetailsMatching(allUserPredicate)
    getClustersFromNodeMetas(conf, javaSet.toSet)
  }

  def getClustersFromNodeMetas(conf: Map[String, String], nodes: Set[NodeMetadata]) : Iterable[JcloudCluster] = {
    val clusterMasters = nodes.filter(!_.getUserMetadata.containsKey(AwsConstants.master))
      .map(nm => (AwsUtilities.stripRegion(conf, nm.getId), new JcloudCluster(nm, collection.mutable.Set()))).toMap

    nodes.filter(_.getUserMetadata.containsKey(AwsConstants.master)).foreach(s => {
      val masterId = s.getUserMetadata.get(AwsConstants.master)
      val master = clusterMasters.get(masterId)
      if (master.isEmpty) {
        val slaveId = AwsUtilities.stripRegion(conf, s.getId)
        logger.warn(s"Error, master node of $slaveId master not found: $masterId, ignoring node.")
      } else {
        master.get.slaves.add(s)
      }
    })
    clusterMasters.values
  }

  //Helper methods for NodeMeta.  TODO- use AwsNodeMeta modeled class instead of jclouds class internally.

  def ipAddress(nm : NodeMetadata) : String = {
    val publicAddresses = nm.getPublicAddresses
    if (publicAddresses.size() == 0) {
      ""
    } else {
      nm.getPublicAddresses.iterator().next()
    }
  }

  def privateIp(nm: NodeMetadata) : String = {
    val privateAddresses = nm.getPrivateAddresses
    if (privateAddresses.size() == 0) {
      ""
    } else {
      privateAddresses.iterator.next
    }
  }


  def stripRegion(conf: Map[String, String], idString: String) : String = {
    val region = getAwsProp(conf, AwsConstants.region)
    idString.stripPrefix(s"$region/")
  }

  def getFullId(conf: Map[String, String], idString: String) : String = {
    val region = getAwsProp(conf, AwsConstants.region)
    s"$region/$idString"
  }

  /**
    * Somewhat hacky, instead of passing address everywhere, we pass it through a configuration
    * even if it provided programatically and not read from the user config.
    *
    * That way, we have less code, and just have one code path that reads from configuration.
    * Anyway it's hidden through Node Factory API's
    *
    */
  def getConfWithIp(conf: Map[String, String], ipAddress: String) : Map[String, String] = {
    val newConfMutable = collection.mutable.Map[String, String] (conf.toArray:_*)
    newConfMutable.+= (AwsConstants.getAddressFull -> ipAddress)
    newConfMutable.toMap
  }

  //Helper method for user-facing AwsNodeMeta class

  private def getAwsNodeMeta(conf: Map[String, String], nm: NodeMetadata) : AwsNodeMeta = {
    AwsNodeMeta(id = stripRegion(conf, nm.getId),
      publicIp = ipAddress(nm),
      privateIp = privateIp(nm),
      status = getStatus(nm.getStatus),
      shortHostName = nm.getUserMetadata.get(AwsConstants.hostName)
    )
  }

  private def getStatus(status: NodeMetadata.Status) : AwsNodeState = {
    status match {
      case Status.ERROR => AwsError
      case Status.PENDING => AwsPending
      case Status.RUNNING => AwsRunning
      case Status.TERMINATED => AwsTerminated
      case Status.SUSPENDED => AwsSuspended
      case _ => new OtherAwsState(status.toString)
    }
  }

  def getAwsCluster(conf: Map[String, String], cluster: JcloudCluster) : AwsCluster = {
    val master = getAwsNodeMeta(conf, cluster.master)
    val slaves = cluster.slaves.map(s => getAwsNodeMeta(conf, s)).toSet

    val createTime = cluster.master.getUserMetadata.get(AwsConstants.createTime)
    val expireTime = cluster.master.getUserMetadata.get(AwsConstants.expireTime)
    val user = cluster.master.getUserMetadata.get(AwsConstants.userTagKey)
    val tags = getTags(cluster.master)
    AwsCluster(master, slaves, createTime, expireTime, tags, user)
  }

  def setTag(conf: Map[String, String], cluster: JcloudCluster, tagName: String, tagValue: Option[String]) = {
    val region = AwsUtilities.getAwsProp(conf, AwsConstants.region)
    val tagApi = AwsUtilities.getEc2Api(conf).getTagApiForRegion(region).get()
    val masterId = AwsUtilities.stripRegion(conf, cluster.master.getId)
    val meta = cluster.master.getUserMetadata
    tagValue match {
      case None => {
        tagApi.deleteFromResources(List(tagName).asJava, List(masterId).asJava)
      }
      case Some(v) => {
        meta.put(tagName, v)
        tagApi.applyToResources(meta, List(masterId).asJava)
      }
    }
  }

  def getTags(awsNode: NodeMetadata) : Map[String, String] = {
    val meta = awsNode.getUserMetadata.toMap
    val tags = meta.filter{case (k,v) => k.startsWith(AwsConstants.userTagPrefix)}
    tags.map{case(k,v) => (k.stripPrefix(AwsConstants.userTagPrefix), v)}
  }

  //-----
  //AWS instance timestamp.  All timestamps stored in UTC.
  //-----

  def updateExpireTime(dtString : String) : String = {
    val dt = stringToDt(dtString)
    val expiryTime = dt + AwsConstants.extensionTime
    dtToString(expiryTime)
  }

  def getCurrentTime() : String = {
    val dt = DateTime.now(DateTimeZone.UTC)
    dtToString(dt)
  }

  def dtToString(dt: DateTime) : String = {
    DateTimeFormat.forPattern(AwsConstants.dateTimeFormat).print(dt)
  }

  def stringToDt(dtString: String) : DateTime = {
    //Important to parse the string as UTC, so the miliseconds value is correct!
    DateTimeFormat.forPattern(AwsConstants.dateTimeFormat).withZoneUTC().parseDateTime(dtString)
  }

  //--- Printing some help commands for AWS nodes.

  def printImageInfo(imageGroup: ImageGroup) = {
    val master = imageGroup.master

    val user = master.getTags.get(AwsConstants.userTagKey)
    val createTime = BucketUtilities.getReadableFromSortableDt(master.getTags.get(AwsConstants.createTime))
    println(s"Master Image ${master.getId}, RawState=${master.getRawState}")
    println(s"\tCreated by: $user at $createTime (UTC)")
    println(s"\tDescription: ${master.getDescription}")

    val slaveOption = imageGroup.slave
    if (slaveOption.isDefined) {
      val slave = slaveOption.get
      println(s"\tWith Slave Image ${slave.getId}, RawState=${slave.getRawState}")
    }
  }

  def printClusterInfo(conf: Map[String, String], cluster: JcloudCluster, includeOwner: Boolean = false) = {
    val id = AwsUtilities.stripRegion(conf, cluster.master.getId())
    println(s"Cluster [$id], size = ${cluster.size} node(s)")

    //print tags
    if (includeOwner) {
      val owner = cluster.master.getUserMetadata.get(AwsConstants.userTagKey)
      if (owner != null) {
        println(s"Owned by: $owner")
      }
    }
    val tags = getTags(cluster.master)
    if (tags.size > 0) {
      println("Tags")
    }
    tags.foreach{case(k,v) => println(s"\t$k = $v")}

    //print cluster
    print("Master ")
    println(AwsUtilities.nodeInformation(conf, cluster.master))

    if (cluster.master.getStatus().equals(NodeMetadata.Status.RUNNING)) {
      AwsUtilities.printNodeInfoFull(conf, cluster.master, NodeRole.Master)
    }

    cluster.slaves.foreach(s => {
      println("\tSlave " + AwsUtilities.nodeInformation(conf, s))
      if (s.getStatus().equals(NodeMetadata.Status.RUNNING)) {
        AwsUtilities.printNodeInfoFull(conf, s, NodeRole.Slave)
      }
      println
    })
  }

  def nodeInformation(conf: Map[String, String], u : NodeMetadata) : String = {
    val sb = new StringBuilder(s"Instance id: [${AwsUtilities.stripRegion(conf, u.getId())}], state: ${u.getStatus()}")
    val shortHostName = u.getUserMetadata.get(AwsConstants.hostName)
    val isSlave = u.getUserMetadata.get(AwsConstants.master) != null
    if (shortHostName != null) {
      sb.append(s", hostName: [$shortHostName]")
    }
    if (u.getStatus() == Status.RUNNING) {
      val ip = ipAddress(u)
      sb.append(s", ip: [$ip]")
      if (!isSlave) {
        sb.append(s", created: ${u.getUserMetadata.get(AwsConstants.createTime)} (UTC)" +
          s", expires: ${u.getUserMetadata.get(AwsConstants.expireTime)} (UTC)")
      }
    }
    sb.toString
  }

  def printNodeInfoFull(conf: Map[String, String], nodeMeta: NodeMetadata, nodeRole: NodeRole) = {
    val shortId = stripRegion(conf, nodeMeta.getId)
    val ipAddr = ipAddress(nodeMeta)
    val user = AwsUtilities.getAwsProp(conf, GeneralConstants.user)
    val key = AwsUtilities.getAwsProp(conf, GeneralConstants.keyFile)

    //when printing out slave, tab so it is under the master.
    val tab = nodeRole match {
      case NodeRole.Slave => "\t"
      case _ => ""
    }

    println(tab + "%-20s%s".format("SSH command:", s"ssh -i $key $user@$ipAddr"))

    val ports = nodeRole match {
      case NodeRole.Slave => GeneralConstants.slavePorts
      case _ => GeneralConstants.masterPorts
    }


    ports.foreach(p => {
      println(tab + "%-20s%s".format(s"${p._2}:", s"$ipAddr:${p._1}"))
    })
  }


  //--- For create-image-aws

  def getImageName: String = {
    val random = new Random()
    val randomInt = random.nextInt(10000)
    s"dev_cluster_image_${randomInt}"
  }

  def getUserImagesRaw(conf: Map[String, String], ec2Api: EC2Api) : Iterable[Image] = {
    val region = AwsUtilities.getAwsProp(conf, AwsConstants.region)
    val amiApi = ec2Api.getAMIApi.get
    val ownerId = AwsUtilities.getAwsProp(conf, AwsConstants.accountId)
    val mm = ImmutableSetMultimap.of("owner-id", ownerId)
    val images = amiApi.describeImagesInRegionWithFilter(region, mm)
    val user = System.getenv("USER")
    images.asScala.filter(i => {
      val tags = i.getTags
      tags.containsKey(AwsConstants.groupTag) && tags.containsKey(AwsConstants.userTagKey) && tags.get(AwsConstants.userTagKey).equals(user)
    })
  }

  def getUserImages(conf: Map[String, String], ec2Api: EC2Api) : Iterable[ImageGroup] = {
    val images = getUserImagesRaw(conf, ec2Api)

    //create image-group using the list of master images (images without 'master' pointer tag)
    val imageMap = images.filter(i => !isSlaveImage(i)).map(m =>
      (m.getId, new ImageGroup(m, None))).toMap

    //populate with slave image (images with 'master' pointer tag)
    images.filter(i => isSlaveImage(i)).filter(s => imageMap.get(getMasterImage(s)).isDefined)
      .foreach(s => {
        val imageGroup = imageMap.get(getMasterImage(s)).get
        imageGroup.slave = Some(s)
    })

    //add slaves without masters (images with 'master' pointer tag, but whose master does not exist)
    val orphans = images.filter(i => isSlaveImage(i)).filter(s =>
      !imageMap.containsKey(s.getTags.get(AwsConstants.master))).map(s => new ImageGroup(s, None))
    (imageMap.map(_._2) ++ orphans)
  }

  def getUserImage(conf: Map[String, String], ec2Api: EC2Api, tgtImage: String) : ImageGroup = {
    val userImages = getUserImages(conf, ec2Api)
    val result = userImages.filter(ui => ui.master.getId.equals(tgtImage))
    require (result.size == 1, "Image ids are unique")
    result.last
  }

  def isSlaveImage(i: Image) = {
    i.getTags.containsKey(AwsConstants.master)
  }

  def getMasterImage(slaveImage : Image) = {
    slaveImage.getTags.get(AwsConstants.master)
  }

  def isImageReady(i: ImageGroup) = {
    val available = "available"  //hope AWS state stays like this.  Not sure why JClouds doesn't offer an enum..
    i.master.getRawState.equalsIgnoreCase(available) &&
      i.slave.isDefined && i.slave.get.getRawState.equalsIgnoreCase(available)
  }

  //Generic setup stuff

  def getOsSetupScript(conf: Map[String, String], nodeRole: NodeRole) = {
    val osString = GeneralUtilities.getConfStrict(conf, GeneralConstants.os, GeneralConstants.targetCommonProps)
    osString match {
      case GeneralConstants.ubuntu_trusty => {
        nodeRole match {
          case NodeRole.Master => "ubuntu/setup-master.sh"
          case NodeRole.Slave => "ubuntu/setup-slave.sh"
          case _ => throw new IllegalArgumentException("Internal error, undefined node type")
        }
      }
      case _ => throw new IllegalArgumentException(s"Sorry only ${GeneralConstants.ubuntu_trusty} images are generated for now.")
    }
  }

  def getStartServiceScript(conf: Map[String, String], nodeRole: NodeRole) = {
    nodeRole match {
      case NodeRole.Master => "start/start-master.sh"
      case NodeRole.Slave => "start/start-slave.sh"
      case _ => throw new IllegalArgumentException("Internal error, undefined node type")
    }
  }


  object NodeRole extends Enumeration {
    type NodeRole = Value
    val Master = Value("master")
    val Slave = Value("slave")
  }

  /**
    * AWS is flaky, so retry some of the actions.
    *
    * @param retryable retryable action
    * @tparam A
    * @return
    */
  def retryAwsAction[A >: Null <: Any] (retryable : Retryable[A]) : A = {
    //try 3 times, delay 30 seconds
    var retries = 3
    var delay = 5000


    var res : A = null
    while(res == null) {
      retries = retries - 1
      try {
        res = retryable.action()
      } catch {
        case t: Throwable if retries > 0 => {
          logger.info(s"Recevied error $t, retrying in $delay ms")
          Thread.sleep(delay)
        }
      }
    }
    res
  }
}

trait Retryable[A >: Null] {
  def action() : A
}

case class JcloudCluster(master: NodeMetadata, slaves: scala.collection.mutable.Set[NodeMetadata] = scala.collection.mutable.Set[NodeMetadata]()) {
  def size = 1 + slaves.size
}

class ImageGroup(var master: Image, var slave: Option[Image])