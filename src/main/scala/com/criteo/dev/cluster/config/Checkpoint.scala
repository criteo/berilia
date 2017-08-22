package com.criteo.dev.cluster.config

import java.time.Instant

/**
  * Checkpoint for data copy
  * @param created
  * @param updated
  * @param todo Tables to be copied
  * @param finished Tables copied with success
  * @param failed Tables failed to copy
  * @param invalid Invalid tables
  */
case class Checkpoint(
                       created: Instant,
                       updated: Instant,
                       todo: Set[String] = Set.empty,
                       finished: Set[String] = Set.empty,
                       failed: Set[String] = Set.empty,
                       invalid: Set[String] = Set.empty
                     )
