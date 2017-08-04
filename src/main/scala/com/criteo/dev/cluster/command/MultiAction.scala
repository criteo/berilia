package com.criteo.dev.cluster.command

trait MultiAction {
  def add(command: String): Unit
  def run(returnResult: Boolean = false, ignoreError: Boolean = false): String
}
