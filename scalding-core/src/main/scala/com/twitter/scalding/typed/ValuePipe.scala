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


object ValuePipe extends java.io.Serializable {
  implicit def toPipe[V](v: ValuePipe[V]): TypedPipe[V] = v.toPipe

  def fold[T, U, V](l: ValuePipe[T], r: ValuePipe[U])(f: (T, U) => V): ValuePipe[V] =
    l.leftCross(r).collect { case (t, Some(u)) => f(t,u) }

  implicit def semigroup[T](implicit sg: Semigroup[T]): Semigroup[ValuePipe[T]] =
    new Semigroup[ValuePipe[T]] {
      def plus(l: ValuePipe[T], r: ValuePipe[T]) = fold(l, r)(sg.plus)
    }
}

/** ValuePipe is special case of a TypedPipe of just a optional single element.
  *  It is like a distribute Option type
  * It allows to perform scalar based operations on pipes like normalization.
  */
sealed trait ValuePipe[+T] extends java.io.Serializable {
  def leftCross[U](that: ValuePipe[U]): ValuePipe[(T, Option[U])] = that match {
    case EmptyValue() => map((_, None))
    case LiteralValue(v2) => map((_, Some(v2)))
    case _ => ComputedValue(toPipe.leftCross(that))
  }
  def collect[U](fn: PartialFunction[T, U]): ValuePipe[U] =
    filter(fn.isDefinedAt(_)).map(fn(_))

  def map[U](fn: T => U): ValuePipe[U]
  def filter[U](fn: T => Boolean): ValuePipe[T]
  def toPipe: TypedPipe[T]
}
case class EmptyValue(implicit val flowDef: FlowDef, mode: Mode) extends ValuePipe[Nothing] {
  override def leftCross[U](that: ValuePipe[U]) = EmptyValue()
  def map[U](fn: Nothing => U): ValuePipe[U] = EmptyValue()
  def filter[U](fn: Nothing => Boolean) = EmptyValue()
  def toPipe: TypedPipe[Nothing] = TypedPipe.empty
}
case class LiteralValue[T](value: T)(implicit val flowDef: FlowDef, mode: Mode) extends ValuePipe[T] {
  def map[U](fn: T => U) = LiteralValue(fn(value))
  def filter[U](fn: T => Boolean) = if(fn(value)) this else EmptyValue()
  lazy val toPipe = TypedPipe.from(Iterable(value))
}
case class ComputedValue[T](override val toPipe: TypedPipe[T]) extends ValuePipe[T] {
  def map[U](fn: T => U) = ComputedValue(toPipe.map(fn))
  def filter[U](fn: T => Boolean) = ComputedValue(toPipe.filter(fn))
}
