package com.twitter.scalding.serialization

sealed trait RequireOrderedSerializationMode extends Serializable
object RequireOrderedSerializationMode {
  case object Fail extends RequireOrderedSerializationMode
  case object Log extends RequireOrderedSerializationMode
}
