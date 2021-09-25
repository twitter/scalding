package com.twitter.scalding.serialization

import com.twitter.scalding.{ Config, Job }

sealed trait RequireOrderedSerializationMode
object RequireOrderedSerializationMode {
  case object Fail extends RequireOrderedSerializationMode
  case object Log extends RequireOrderedSerializationMode
}

trait RequiredBinaryComparatorsConfig extends Job {
  def requireOrderedSerializationMode: RequireOrderedSerializationMode = RequireOrderedSerializationMode.Fail
  override def config = super.config + (Config.ScaldingRequireOrderedSerialization -> requireOrderedSerializationMode.toString)
}
