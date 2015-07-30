package com.twitter.scalding.serialization

import com.twitter.scalding.{ Config, Job }

trait RequiredBinaryComparatorsConfig extends Job {
  override def config = super.config + (Config.ScaldingRequireOrderedSerialization -> "true")
}
