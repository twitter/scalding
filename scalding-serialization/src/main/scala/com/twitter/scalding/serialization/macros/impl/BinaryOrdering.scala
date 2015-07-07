package com.twitter.scalding.serialization.macros.impl

import com.twitter.scalding.serialization.OrderedSerialization

import scala.language.experimental.macros

/**
 * @author Mansur Ashraf.
 */
object BinaryOrdering {

  implicit def ordSer[T]: OrderedSerialization[T] = macro com.twitter.scalding.serialization.macros.impl.OrderedSerializationProviderImpl[T]
}
