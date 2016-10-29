package com.criteo.dev.cluster


import com.criteo.dev.cluster.copy.CopyConstants
import com.criteo.dev.cluster.docker.DockerConstants
import com.criteo.dev.cluster.aws.AwsConstants
import org.slf4j.LoggerFactory

import scala.xml._

/**
  * Configuration of dev-cluster program is done via xml file, which is a collection of
  * "property" elements with "name", "value", and "description" tag.
  *
  * Supports concept of a profile elements (collection of properties).  These are actived either by passed in flags
  * or setting them as default.  However, only one may be chosen at each level.
  */
object ConfigManager {

  private val logger = LoggerFactory.getLogger(ConfigManager.getClass)

  @Public
  def load(configuredProfiles: List[String]) = {
    val conf = scala.collection.mutable.Map[String, String]()
    val home = GeneralUtilities.getHomeDir
    loadConfigs(s"$home/${GeneralConstants.targetLocalProps}", configuredProfiles, conf)
    loadConfigs(s"$home/${GeneralConstants.targetAwsProps}", configuredProfiles, conf)
    loadConfigs(s"$home/${GeneralConstants.gatewayProps}", configuredProfiles, conf)
    loadConfigs(s"$home/${GeneralConstants.sourceProps}", configuredProfiles, conf)
    loadConfigs(s"$home/${GeneralConstants.targetCommonProps}", configuredProfiles, conf)

    conf.toMap
  }

  /**
    * Traverses this file to find all properties.
    *
    * @param location file location
    * @param configuredProfiles profiles are removed once they are used.
    * @return map of all property elements found in this file
    */
  private def loadConfigs(location: String, configuredProfiles: List[String],
                  conf: collection.mutable.Map[String, String]) : Unit = {
    val xml = XML.loadFile(location)
    evalXmlBlock(location, xml, configuredProfiles, conf)
  }

  /**
    * Performs a dfs traversal of given xml element.
    *
    * @param location file location (used in error message when parsing)
    * @param xml xml element to traverse
    * @param configuredProfiles property profiles configured by user
    * @return map of properties found during tree traversal of this element.
    */
  private def evalXmlBlock(location: String, xml: NodeSeq, configuredProfiles: List[String],
                           conf: collection.mutable.Map[String, String]) : Unit = {

    //add properties to the map
    val properties = xml \ "property"
    properties.foreach(x => {
      try {
        val name = (x \ "name").text
        val value = (x \ "value").text
        if (conf.get(name).isDefined) {
          throw new ConfigException(s"Duplicated key $name defined evaluating given profiles: "
            + configuredProfiles.mkString(","))
        }
        conf.+=(name -> value)
      } catch {
        case e: IllegalArgumentException =>
          throw new IllegalArgumentException(s"Error parsing properties on $location", e)
      }
    })

    //find profiles. Only one profile can be activated for a level.
    val profileNodes = (xml \ "profile")
    val profiles = profileNodes.map(p => {
      try {
        val default = {
          //no default defined, treat it as false.
          if ((p \ "default").length == 0) false else (p \ "default").text.toBoolean
        }
        Profile(
          (p \ "id").text,
          default,
          p
        )
      } catch {
        case e: IllegalArgumentException => throw new IllegalArgumentException(s"Error parsing profiles on $location", e)
      }
    })

    //check default profiles if violate 1 per level rule.
    val defaultProfiles = profiles.filter(_.default)
    if (defaultProfiles.size > 1) {
      throw new IllegalArgumentException("Error, more than one profile in same level were marked as default: " +
        s"${defaultProfiles.map(_.id).mkString(",")}")
    }

    val activeProfiles = profiles.filter(p => {
      configuredProfiles.contains(p.id)
    })

    //choose profile.  Priority on configured profile, then on default.
    val chosenProfiles = {
      if (activeProfiles.size > 0) {
        activeProfiles.map(_.id).foreach(id => {
          logger.info(s"Choosing specified property profiles: $id in file $location")
        })
        activeProfiles
      } else if (defaultProfiles.size == 1) {
        val chosen = defaultProfiles.last
        logger.info(s"Choosing default property profile: ${chosen.id} in file $location")
        List(defaultProfiles.last)
      } else {
        List()
      }
    }

    chosenProfiles.foreach(p => {
      evalXmlBlock(location, p.nodeSeq, configuredProfiles, conf)
    })
  }
}

case class Profile(id: String, default: Boolean, nodeSeq: NodeSeq)

class ConfigException(msg: String) extends RuntimeException(msg)
