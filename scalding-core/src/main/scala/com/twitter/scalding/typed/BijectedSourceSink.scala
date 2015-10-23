/*
Copyright 2012 Twitter, Inc.

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

import cascading.flow.FlowDef
import cascading.pipe.Pipe

import com.twitter.bijection.ImplicitBijection
import com.twitter.scalding._
import serialization.Externalizer

import scala.annotation.meta.param

object BijectedSourceSink {
  type SourceSink[T] = TypedSource[T] with TypedSink[T]
  def apply[T, U](parent: SourceSink[T])(implicit transformer: ImplicitBijection[T, U]): BijectedSourceSink[T, U] =
    new BijectedSourceSink(parent)(transformer)
}

class BijectedSourceSink[T, U](parent: BijectedSourceSink.SourceSink[T])(implicit @(transient @param) transformer: ImplicitBijection[T, U]) extends TypedSource[U] with TypedSink[U] {

  val lockedBij = Externalizer(transformer)

  def setter[V <: U] = parent.setter.contraMap(lockedBij.get.invert(_))

  override def converter[W >: U] = parent.converter.andThen{ t: T => lockedBij.get(t) }: TupleConverter[W]

  override def read(implicit flowDef: FlowDef, mode: Mode): Pipe = parent.read
  override def writeFrom(pipe: Pipe)(implicit flowDef: FlowDef, mode: Mode) = parent.writeFrom(pipe)
}
