package com.criteo.dev.cluster.command

trait HiveAction {
  def run(): String
  def add(action: String): Unit
}
