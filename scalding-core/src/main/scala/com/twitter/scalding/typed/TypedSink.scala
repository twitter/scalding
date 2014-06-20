/*
Copyright 2013 Twitter, Inc.

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

package com.twitter.scalding.typed

import com.twitter.scalding._

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.tuple.Fields

object TypedSink extends java.io.Serializable {
  /**
   * Build a TypedSink by declaring a concrete type for the Source
   * Here because of the late addition of TypedSink to scalding to make it
   * easier to port legacy code
   */
  def apply[T](s: Source)(implicit tset: TupleSetter[T]): TypedSink[T] =
    new TypedSink[T] {
      def setter[U <: T] = TupleSetter.asSubSetter[T, U](tset)
      def writeFrom(pipe: Pipe)(implicit flowDef: FlowDef, mode: Mode): Pipe =
        s.writeFrom(pipe)
    }
}

/**
 * Opposite of TypedSource, used for writing into
 */
trait TypedSink[-T] extends java.io.Serializable {
  def setter[U <: T]: TupleSetter[U]
  // These are the fields the write function is expecting
  def sinkFields: Fields = Dsl.intFields(0 until setter.arity)

  /**
   * pipe is assumed to have the schema above, otherwise an error may occur
   * The exact same pipe is returned to match the legacy Source API.
   */
  def writeFrom(pipe: Pipe)(implicit flowDef: FlowDef, mode: Mode): Pipe

  /**
   * Transform this sink into another type by applying a function first
   */
  def contraMap[U](fn: U => T): TypedSink[U] = {
    val self = this // compiler generated self can cause problems with serialization
    new TypedSink[U] {
      override def sinkFields = self.sinkFields
      def setter[V <: U]: TupleSetter[V] = self.setter.contraMap(fn)
      def writeFrom(pipe: Pipe)(implicit fd: FlowDef, mode: Mode): Pipe = self.writeFrom(pipe)
      override def contraMap[U1](fn2: U1 => U) = self.contraMap(fn2.andThen(fn))
    }
  }
}

