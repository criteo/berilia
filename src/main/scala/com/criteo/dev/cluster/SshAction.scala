package com.criteo.dev.cluster

import com.criteo.dev.cluster.aws.AwsUtilities
import org.slf4j.LoggerFactory

import scala.sys.process.{Process, ProcessLogger}

/**
  * Executes a remote script.
  */
@Public
object SshAction {
  private val logger = LoggerFactory.getLogger(SshAction.getClass)

  private val suppressingProcessLogger =  ProcessLogger(_ => ())

  private val processLogger = ProcessLogger(
    (o: String) => logger.info(o),
    (e: String) => logger.error(e))



  /**
    * @param node ssh target
    * @param script script to run remotely
    * @param returnResult whether we need the result.
    * @param ignoreFailure whether it is a best effort.
    * @return Output if 'returnResult' is true, else null
    */
  def apply(node: Node, script: String, returnResult: Boolean = false, ignoreFailure: Boolean = false): String = {
    val command = s"ssh -o StrictHostKeyChecking=no ${GeneralUtilities.nodeString(node)} $script"

    val commands = command.split("\\s+")
    val p = DevClusterProcess.processSeq(commands)

    (returnResult, ignoreFailure) match {
      case (false, true) => {
        p.!(suppressingProcessLogger)
        return null
      }
      case (false, false) => {
        GeneralUtilities.checkStatus(p.!(processLogger))
        return null
      }
      case (true, true) => {
        try {
          p.!!(suppressingProcessLogger)
        } catch {
          case e: Exception => null
        }
      }
      case (true, false) => return p.!!(processLogger)
    }
  }

}
