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

import com.twitter.algebird._
import com.twitter.scalding.{Mode, IterableSource}
import cascading.flow.FlowDef


object ValuePipe {
  implicit def toPipe[V](v: ValuePipe[V]): TypedPipe[V] = v.toPipe

  def fold[T, U, V](l: ValuePipe[T], r: ValuePipe[U])(f: (T, U) => V): ValuePipe[V] =
    new ComputedValue(l.toPipe.cross(r.toPipe).map{ case (lv, rv) => f(lv, rv) })

  implicit def semigroup[T](implicit sg: Semigroup[T]): Semigroup[ValuePipe[T]] =
    new Semigroup[ValuePipe[T]] {
      def plus(l: ValuePipe[T], r: ValuePipe[T]) = fold(l, r)(sg.plus)
    }
}

/** ValuePipe is special case of a TypedPipe of just a single element.
  * It allows to perform scalar based operations on pipes like normalization.
  */
sealed trait ValuePipe[+T] {
  def map[U](fn: T => U): ValuePipe[U]
  def toPipe: TypedPipe[T]
}
case class LiteralValue[T](value: T)(implicit val flowDef: FlowDef, mode: Mode) extends ValuePipe[T] {
  def map[U](fn: T => U) = LiteralValue(fn(value))
  lazy val toPipe = TypedPipe.from(Iterable(value))
}
case class ComputedValue[T](override val toPipe: TypedPipe[T]) extends ValuePipe[T] {
  def map[U](fn: T => U) = ComputedValue(toPipe.map(fn))
}
