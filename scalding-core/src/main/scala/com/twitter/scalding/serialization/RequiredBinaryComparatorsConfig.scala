package com.twitter.scalding.serialization

import com.twitter.scalding.{Config, Job}

trait RequiredBinaryComparatorsConfig extends Job {
  def requireOrderedSerializationMode: RequireOrderedSerializationMode = RequireOrderedSerializationMode.Fail
  override def config =
    super.config + (Config.ScaldingRequireOrderedSerialization -> requireOrderedSerializationMode.toString)
}
