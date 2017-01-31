package com.criteo.dev.cluster.aws

import com.criteo.dev.cluster.GeneralConstants
import org.scala_tools.time.Implicits._

/**
  * Constants for AWS
  */
object AwsConstants {

  //Aws tags- used to track instances spawned by this program
  def groupTag = "devcluster"

  def userTagKey = "dev.user"

  def expireTime = "expire.time"

  def createTime = "create.time"

  def master = "cluster.master"

  //slaves point to the master, master is null.
  def hostName = "hostName"

  //we give each instance a readable host name, configured in cluster's /etc/hosts.
  def roleTag = "image.role"

  def userTagPrefix = "user.tag."


  //node configuration keys
  def awsKeyPrefix = "target.aws"

  def bucketKeyPrefix = "target.bucket" //internally used to pass bucket spec's

  def accessKey = "access.key"

  def accessId = "access.id"

  def accountId = "account.id"

  def instanceType = "instance.type"

  def volumeSpec = "volume.spec"

  def autoVolume = "auto.volumes"

  def getFull(arg: String): String = s"${AwsConstants.awsKeyPrefix}.$arg"

  def getAddressFull: String = getFull(GeneralConstants.address)

  def getUserFull: String = getFull(GeneralConstants.user)

  def getKeyFileFull: String = getFull(GeneralConstants.keyFile)

  def region = "region"

  def masterImageId = s"${AwsConstants.awsKeyPrefix}.master.image.id"

  def slaveImageId = s"${AwsConstants.awsKeyPrefix}.slave.image.id"

  def baseImageId = s"${AwsConstants.awsKeyPrefix}.base.image.id"

  def securityGroup = "security.group"

  def subnet = "subnet"

  def keyPair = "key.pair"


  //timestamp stuff
  def dateTimeFormat = "dd/MMM/yyyy HH:mm:ss"

  def extensionTime = 3 days

  //make this configurable?
  def twiceExtensionTime = (2 * 3) days


  def copyLocation = "/copy"
}
