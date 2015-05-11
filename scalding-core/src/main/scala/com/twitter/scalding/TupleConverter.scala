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

package com.twitter.scalding

trait TupleConverter[@specialized(Int, Long, Float, Double) T] extends java.io.Serializable with TupleArity { self =>
  def apply(te: Seq[String]): T
  def andThen[U](fn: T => U): TupleConverter[U] = new TupleConverter[U] {
    def apply(te: Seq[String]) = fn(self(te))
    def arity = self.arity
  }
}

trait LowPriorityTupleConverters extends java.io.Serializable {
  implicit def singleConverter[@specialized(Int, Long, Float, Double) A](implicit g: TupleGetter[A]) =
    new TupleConverter[A] {
      def apply(tup: Seq[String]) = g.get(tup, 0)
      def arity = 1
    }
}

object TupleConverter extends GeneratedTupleConverters {
  /**
   * Treat this TupleConverter as one for a superclass
   * We do this because we want to use implicit resolution invariantly,
   * but clearly, the operation is covariant
   */
  def asSuperConverter[T, U >: T](tc: TupleConverter[T]): TupleConverter[U] = tc.asInstanceOf[TupleConverter[U]]

  def build[T](thisArity: Int)(fn: Seq[String] => T): TupleConverter[T] = new TupleConverter[T] {
    def apply(te: Seq[String]) = fn(te)
    def arity = thisArity
  }
  def fromTuple[T](t: Seq[String])(implicit tc: TupleConverter[T]): T = tc(t)
  def arity[T](implicit tc: TupleConverter[T]): Int = tc.arity
  def of[T](implicit tc: TupleConverter[T]): TupleConverter[T] = tc

  implicit lazy val UnitConverter: TupleConverter[Unit] = new TupleConverter[Unit] {
    override def apply(arg: Seq[String]) = ()
    override def arity = 0
  }
}
