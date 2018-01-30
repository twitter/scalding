package com.twitter.scalding.serialization

import com.twitter.scalding._

import scala.language.experimental.{ macros => smacros }

/**
 * RequiredBinaryComparators provide comparators (or Ordering in Scala) that are capable of comparing keys in their
 * serialized form reducing the amount of time spent in serialization/deserialization.  These comparators are implemented
 * using Scala macros, and currently provide binary comparators for primitives, strings, Options, tuples, collections, case classes
 * and Scrooge objects.
 */
trait RequiredBinaryComparators extends RequiredBinaryComparatorsConfig {

  implicit def ordSer[T]: OrderedSerialization[T] = macro com.twitter.scalding.serialization.macros.impl.OrderedSerializationProviderImpl[T]

}

object RequiredBinaryComparators {

  implicit def orderedSerialization[T]: OrderedSerialization[T] = macro com.twitter.scalding.serialization.macros.impl.OrderedSerializationProviderImpl[T]
}

/**
 * Use this for an ExecutionApp.
 */
trait RequiredBinaryComparatorsExecutionApp extends ExecutionApp {
  implicit def ordSer[T]: OrderedSerialization[T] = macro com.twitter.scalding.serialization.macros.impl.OrderedSerializationProviderImpl[T]
  def requireOrderedSerializationMode: RequireOrderedSerializationMode = RequireOrderedSerializationMode.Fail
  override def config(inputArgs: Array[String]): (Config, Mode) = {
    val (conf, m) = super.config(inputArgs)
    (conf.setRequireOrderedSerializationMode(Some(requireOrderedSerializationMode)), m)
  }
}
