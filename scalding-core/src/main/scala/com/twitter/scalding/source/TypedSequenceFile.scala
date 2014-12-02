/*
Copyright 2014 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.twitter.scalding.source

import cascading.tuple.Fields
import com.twitter.scalding._
import com.twitter.scalding.SequenceFile

/**
 * SequenceFile with explicit types. Useful for debugging flows using the Typed API.
 * Not to be used for permanent storage: uses Kryo serialization which may not be
 * consistent across JVM instances. Use Thrift sources instead.
 */
class TypedSequenceFile[T](val path: String) extends SequenceFile(path, Fields.FIRST) with Mappable[T] with TypedSink[T] {
  override def converter[U >: T] =
    TupleConverter.asSuperConverter[T, U](TupleConverter.singleConverter[T])
  override def setter[U <: T] =
    TupleSetter.asSubSetter[T, U](TupleSetter.singleSetter[T])
  override def toString: String = "TypedSequenceFile(%s)".format(path)
  override def equals(that: Any): Boolean = that match {
    case null => false
    case t: TypedSequenceFile[_] => t.p == p // horribly named fields in the SequenceFile case class
    case _ => false
  }
  override def hashCode = path.hashCode
}

object TypedSequenceFile {
  def apply[T](path: String): TypedSequenceFile[T] = new TypedSequenceFile[T](path)
}
