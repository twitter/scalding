package com.twitter.scalding_internal.db.vertica

import java.util.Properties
import com.twitter.scalding_internal.db._
import java.io._

trait VerticaRowSerializer[T] extends java.io.Serializable {
  def serialize(t: T, o: OutputStream): Unit
  def deserialize(i: InputStream): T = sys.error("Not implemented")
}
