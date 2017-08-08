package com.criteo.dev.cluster.command

import com.criteo.dev.cluster.{DevClusterProcess, GeneralUtilities, Public}
import org.slf4j.LoggerFactory

import scala.sys.process.ProcessLogger

/**
  * Execute a shell script
  */
@Public
object ShellAction {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val suppressingProcessLogger = ProcessLogger(_ => ())

  private val processLogger = ProcessLogger(
    (o: String) => logger.info(o),
    (e: String) => logger.error(e))

  def apply(script: String, returnResult: Boolean = false, ignoreFailure: Boolean = false): String = {
    val p = DevClusterProcess.processSeq(script.split("\\s+"))

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
