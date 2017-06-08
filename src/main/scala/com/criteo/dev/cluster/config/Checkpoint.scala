package com.criteo.dev.cluster.config

import java.time.Instant

case class Checkpoint(
                       created: Instant,
                       updated: Instant,
                       todo: Set[String] = Set.empty,
                       finished: Set[String] = Set.empty,
                       failed: Set[String] = Set.empty
                     )
