
package com.twitter.scalding.db

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
