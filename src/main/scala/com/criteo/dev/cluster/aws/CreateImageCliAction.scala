package com.criteo.dev.cluster.aws

import com.criteo.dev.cluster.aws.AwsUtilities.NodeRole
import com.criteo.dev.cluster.config.GlobalConfig
import com.criteo.dev.cluster.s3.BucketUtilities
import com.criteo.dev.cluster.{CliAction, GeneralConstants, GeneralUtilities}
import org.jclouds.compute.domain.NodeMetadata
import org.jclouds.ec2.EC2Api
import org.jclouds.ec2.domain.Image
import org.jclouds.ec2.options.CreateImageOptions
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.collection.JavaConverters._

/**
  * Creates an image for use by create-aws.  Image has Java and Hadoop pre-installed so it will be much faster to run
  * create-aws than create-complete-aws.
  *
  * Note that for now, all configuration, aux jars, are part of the image, so it will need to be re-generated if anything
  * changes.
  *
  * Experimental and currently not enabled, as image maintenance might be more trouble than its worth.
  */
object CreateImageCliAction extends CliAction[Unit] {

  private val logger = LoggerFactory.getLogger(CreateAwsCliAction.getClass)

  override def command: String = "create-image-aws"

  override def usageArgs: List[Any] = List(Option("description"))

  override def help: String = "Creates an image for use by create-aws command.  Image has Java and Hadoop pre-installed so" +
    "it will be faster to run create-aws than create-complete-aws.  Note that all install/config overrides are run as part of " +
    "image creation, so image needs to be regenerated if any change is made."

  override def applyInternal(args: List[String], config: GlobalConfig): Unit = {
    val conf = config.backCompat
    val baseImage = GeneralUtilities.getConfStrict(conf, AwsConstants.baseImageId, GeneralConstants.targetAwsProps)

    //create AWS instance(s) from OS base image.
    val cluster = CreateClusterAction(conf, 2, baseImage, baseImage)

    //configure some hosts
    ConfigureHostsAction(config.target.aws, List(cluster))

    //Install CDH5.5
    InstallHadoopAction(config.target, config.backCompat, cluster)


    val description = if (args.length > 1) {
      Some(args(0))
    } else {
      None
    }
    val ec2Api = AwsUtilities.getEc2Api(conf)
    val masterImage = saveImage(conf, ec2Api, cluster.master, cluster.slaves.last, description)

    logger.info("Waiting on images.  Please wait some minutes before they are available.")
    while (!imagesReady(conf, ec2Api, masterImage)) {
      logger.info("Images not ready.")
      Thread.sleep(10000)
    }
    logger.info("Images are ready.")
    DestroyAwsCliAction.destroy(conf, List(cluster))
  }


  /**
    * Hacky way to busy wait until the images are ready.
    */
  def imagesReady(conf: Map[String, String], ec2Api: EC2Api, masterImage : String) : Boolean = {
    val imageGroup = AwsUtilities.getUserImage(conf, ec2Api, masterImage)
    AwsUtilities.printImageInfo(imageGroup)
    AwsUtilities.isImageReady(imageGroup)
  }

  /**
    * Makes a master and slave image.
    *
    * Also tags them so they are identifiable as an ImageGroup.
    *
    * @param conf configuration
    * @param ec2Api api to talk to Amazon EC2
    * @param master master node metadata (makes master image)
    * @param slave slave node metadata (makes slave image)
    * @param description description to put on the images.
    * @return master image id
    */
  def saveImage(conf: Map[String, String], ec2Api: EC2Api,
                master: NodeMetadata, slave: NodeMetadata,
                description: Option[String]) : String = {
    val region = AwsUtilities.getAwsProp(conf, AwsConstants.region)
    val amiApi = ec2Api.getAMIApi.get
    val createImageOpts = if (description.isDefined) {
      CreateImageOptions.Builder.withDescription(description.get)
    } else {
      new CreateImageOptions
    }

    //create master image
    logger.info("Creating master image.")
    val masterImage: String = amiApi.createImageInRegion(region,
        AwsUtilities.getImageName,
        AwsUtilities.stripRegion(conf, master.getId),
        createImageOpts)
    logger.info(s"Image created from master instance: $masterImage")

    //creating slave image
    logger.info("Creating image from slave instance.")
    val slaveImage: String = amiApi.createImageInRegion(region,
      AwsUtilities.getImageName,
      AwsUtilities.stripRegion(conf, slave.getId),
      createImageOpts)
    logger.info(s"Image created from slave instance: $slaveImage")

    //tag master
    val tagApi = ec2Api.getTagApiForRegion(region).get
    val masterTagMap = Map[String, String] (AwsConstants.groupTag -> "",
      AwsConstants.userTagKey -> System.getenv("USER"),
      AwsConstants.roleTag -> NodeRole.Master.toString,
      AwsConstants.createTime -> BucketUtilities.getSortableDtNow)
    tagApi.applyToResources(masterTagMap.asJava, List(masterImage).asJava)
    logger.info("Master image tagged")

    val slaveTagMap = Map[String, String] (AwsConstants.groupTag -> "",
      AwsConstants.userTagKey -> System.getenv("USER"),
      AwsConstants.roleTag -> NodeRole.Slave.toString,
      AwsConstants.createTime -> BucketUtilities.getSortableDtNow,
      AwsConstants.master -> masterImage)
    tagApi.applyToResources(slaveTagMap.asJava, List(slaveImage).asJava)
    logger.info("Slave image tagged")

    masterImage
  }
}
