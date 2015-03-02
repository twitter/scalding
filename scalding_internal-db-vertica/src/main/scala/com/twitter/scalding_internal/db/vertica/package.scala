package com.twitter.scalding_internal.db
import scala.language.experimental.{ macros => smacros }
import com.twitter.scalding_internal.db.vertica.VerticaRowSerializer

package object vertica {
  implicit def apply[T]: VerticaRowSerializer[T] = macro com.twitter.scalding_internal.db.vertica.macros.VerticaRowSerializerProviderImpl[T]
}