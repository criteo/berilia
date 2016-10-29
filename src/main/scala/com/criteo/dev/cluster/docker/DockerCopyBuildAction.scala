package com.criteo.dev.cluster.docker

import java.io.File

import com.criteo.dev.cluster.{GeneralConstants, GeneralUtilities}
import org.apache.commons.io.FileUtils


/**
  * Docker has some very strict rules when building images, the resources it needs to copy into the image
  * must be in the same directory as the dockerFile.
  *
  * This copies into a temporary directory called ./docker/tmpResources, runs the dockerFile, then deletes
  * the temporary directory.
  *
  * Requirement for dockerFiles that can be built using this action:
  * "ARG resource" is included and used to identify resource to copy.
  *
  * @param dockerFile location of docker file (relative to /dockerFileDir)
  * @param dockerImage target image to build (should be specified in FROM in next dockerfile if used to layer images)
  * @param resourcePath path of resource to add
  */
class DockerCopyBuildAction (dockerFile: String,
                             dockerImage: String,
                             resourcePath: String)
  extends DockerBuildAction (dockerFile, dockerImage) {

  val tempDir = "tmpResources"

  override def run() : Unit = {

    val tmpResourcePath = s"${GeneralUtilities.getHomeDir}/${DockerConstants.dockerBaseDir}/$tempDir"
    val tmpResource = new File(tmpResourcePath)
    GeneralUtilities.prepareDir(tmpResourcePath)

    val resource = new File(s"${GeneralUtilities.getHomeDir}/$resourcePath")
    require (resource.exists(), s"Internal error, resource to copy does not exist: $resourcePath")

    if (resource.isFile()) {
       FileUtils.copyFileToDirectory(resource, tmpResource)
    } else if (resource.isDirectory()) {
       FileUtils.copyDirectory(resource, tmpResource)
    }

    super.addArg(DockerConstants.resource, s"$tempDir")
    super.run()

    FileUtils.deleteDirectory(tmpResource)
  }
}

object DockerCopyBuildAction {
  def apply(dockerFile: String,
            dockerImage: String,
            resourcePath: String) = {
    val obj = new DockerCopyBuildAction(dockerFile, dockerImage, resourcePath)
    obj.run
  }
}
