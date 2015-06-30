package com.twitter.scalding.thrift.macros

import com.twitter.scalding.serialization.{ OrderedSerialization, RequiredBinaryComparatorsConfig }
import com.twitter.scalding.thrift.macros.impl.ScroogeInternalOrderedSerializationImpl
import scala.language.experimental.{ macros => smacros }

/**
 * @author Mansur Ashraf.
 */
trait RequiredBinaryComparators extends RequiredBinaryComparatorsConfig {
  implicit def ordSer[T]: OrderedSerialization[T] = macro ScroogeInternalOrderedSerializationImpl[T]
}
