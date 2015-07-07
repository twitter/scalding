package com.twitter.scalding.db

import java.util.Properties
import java.io._

trait VerticaRowSerializer[T] extends java.io.Serializable {
  def serialize(t: T, o: OutputStream): Unit
  def deserialize(i: InputStream): T = sys.error("Not implemented")
}
