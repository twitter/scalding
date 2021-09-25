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

trait TypedSource[+T] extends java.io.Serializable {
  /**
   * Because TupleConverter cannot be covariant, we need to jump through this hoop.
   * A typical implementation might be:
   * (implicit conv: TupleConverter[T])
   * and then:
   *
   * override def converter[U >: T] = TupleConverter.asSuperConverter[T, U](conv)
   */
  def converter[U >: T]: TupleConverter[U]
  def read(implicit flowDef: FlowDef, mode: Mode): Pipe
  // These are the default column number YOU MAY NEED TO OVERRIDE!
  def sourceFields: Fields = Dsl.intFields(0 until converter.arity)

  /**
   * Transform this TypedSource into another by mapping after.
   * We don't call this map because of conflicts with Mappable, unfortunately
   */
  def andThen[U](fn: T => U): TypedSource[U] = {
    val self = this // compiler generated self can cause problems with serialization
    new TypedSource[U] {
      override def sourceFields = self.sourceFields
      def converter[V >: U]: TupleConverter[V] = self.converter.andThen(fn)
      def read(implicit fd: FlowDef, mode: Mode): Pipe = self.read
      override def andThen[U1](fn2: U => U1) = self.andThen(fn.andThen(fn2))
    }
  }
}
