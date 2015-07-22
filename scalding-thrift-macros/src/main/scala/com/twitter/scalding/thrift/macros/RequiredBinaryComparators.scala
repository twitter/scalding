package com.twitter.scalding.thrift.macros

import com.twitter.scalding.serialization.{ OrderedSerialization, RequiredBinaryComparatorsConfig }
import com.twitter.scalding.thrift.macros.impl.ScroogeInternalOrderedSerializationImpl
import scala.language.experimental.{ macros => smacros }

/**
 * Provides support for Scrooge classes in addition to primitives, cases classes, tuples etc. Use this
 * if you use Scrooge classes as `key` in your scalding job.
 * @author Mansur Ashraf.
 */
trait RequiredBinaryComparators extends RequiredBinaryComparatorsConfig {
  implicit def ordSer[T]: OrderedSerialization[T] = macro ScroogeInternalOrderedSerializationImpl[T]
}
