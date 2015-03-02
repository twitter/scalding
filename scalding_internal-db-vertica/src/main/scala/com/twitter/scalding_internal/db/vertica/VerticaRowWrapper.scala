
package com.twitter.scalding_internal.db.vertica

import java.io._

trait VerticaRowWrapper extends Serializable {
  def sink(os: OutputStream): Unit
}

class VerticaRowWrapperFactory[T](verticaRowSerializer: VerticaRowSerializer[T]) extends Serializable {
  def wrap(t: T): VerticaRowWrapper = {
    new VerticaRowWrapper {
      override def sink(os: OutputStream): Unit =
        verticaRowSerializer.serialize(t, os)

    }
  }
}