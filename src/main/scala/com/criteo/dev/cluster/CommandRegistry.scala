package com.criteo.dev.cluster

import com.criteo.dev.cluster.aws._
import com.criteo.dev.cluster.docker._
import com.criteo.dev.cluster.s3._

/**
  * A registry of commands.
  */
object CommandRegistry {

  private val localCommands = new CliActionCategory(
    "Local Cluster",
    List[CliAction[_]] (
      CreateLocalCliAction,
      DestroyLocalCliAction,
      ListDockerCliAction,
      StartLocalCliAction,
      StopLocalCliAction,
      CopyLocalCliAction,
      AttachBucketLocalCliAction,
      CopyBucketLocalAction
    ),
    Some(GeneralConstants.localClusterType)
  )

  private val awsCommands = new CliActionCategory(
    "AWS Cluster",
    List[CliAction[_]] (
      CreateAwsCliAction,
      DestroyAwsCliAction,
      ListAwsCliAction,
      ListAllAwsCliAction,
      StartAwsCliAction,
      StopAwsCliAction,
      ConfigureAwsCliAction,
      RestartServicesCliAction,
      CopyDirCliAction,
      ExtendAwsCliAction,
      PurgeAwsCliAction,
      CopyAwsCliAction,
      AttachBucketAwsCliAction,
      CopyBucketAwsAction,
      TagAwsCliAction,
      UntagAwsCliAction,
      QueryTagAwsCliAction
    ),
    Some(GeneralConstants.awsType))

  private val gatewayAwsCommands = new CliActionCategory(
    "Docker-Gateway for Remote Cluster (including AWS cluster)",
    List[CliAction[_]] (
      CreateGatewayCliAction,
      ListGatewayCliAction,
      ResumeGatewayCliAction,
      DestroyGatewayCliAction),
    Some(GeneralConstants.awsType))

  private val s3Commands = new CliActionCategory("S3 Buckets", List[CliAction[_]] (
    CreateBucketCliAction,
    DestroyBucketCliAction,
    ListBucketCliAction,
    CopyBucketCliAction,
    DescribeBucketCliAction,
    CopyBucketLocalAction),
    Some(GeneralConstants.s3Type))


  private val miscCommands = new CliActionCategory("Miscellaneous", List[CliAction[_]] (HelpAction))

  private val commands = List[CliActionCategory] (
    localCommands,
    awsCommands,
    gatewayAwsCommands,
    s3Commands,
    miscCommands
  )

  private val commandMap = commands.map(cc => cc.actions).flatten.map(c => (c.command, c)).toMap

  def getCommandMap = commandMap

  def getCommands = commands

  def fillSystemConfs(conf: Map[String, String], command: CliAction[_]) : Map[String, String] = {
    val commandClusterProvider = command.commandCategory.clusterProvider
    if (commandClusterProvider.isDefined) {
      conf + (GeneralConstants.targetTypeProp -> command.commandCategory.clusterProvider.get)
    } else {
      conf
    }
  }
}

case class CliActionCategory(name: String, actions: List[CliAction[_]], clusterProvider: Option[String] =  None) {
  actions.foreach(a => a.commandCategory = this)
}
