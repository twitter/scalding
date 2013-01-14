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

import cascading.tuple.{
  Tuple => CTuple,
  Tuples,
  TupleEntry
}

/** A Type-class that converts FROM cascading TupleEntry objects
 */
abstract class TupleConverter[@specialized(Int,Long,Float,Double)T] extends java.io.Serializable with TupleArity {
  def apply(te : TupleEntry) : T
}

trait LowPriorityTupleConverters extends java.io.Serializable {
  implicit def single[A](implicit g : TupleGetter[A]): TupleConverter[A] =
    new TupleConverter[A] {
        def apply(tup : TupleEntry) = g.get(tup.getTuple, 0)
        def arity = 1
    }
}

object TupleConverter extends GeneratedTupleConverters {

  def convert[T](te: TupleEntry)(implicit tc: TupleConverter[T]): T = tc(te)
  def arity[T](implicit tc: TupleConverter[T]): Int = tc.arity
  def of[T](implicit tc: TupleConverter[T]): TupleConverter[T] = tc

  implicit val unit: TupleConverter[Unit] = new TupleConverter[Unit] {
    override def apply(arg : TupleEntry) = ()
    override def arity = 0
  }

  implicit val tupleEntry: TupleConverter[TupleEntry] = new TupleConverter[TupleEntry] {
    override def apply(tup : TupleEntry) = tup
    override def arity = -1
  }

  implicit val ctuple: TupleConverter[CTuple] = new TupleConverter[CTuple] {
    override def apply(tup : TupleEntry) = Tuples.asUnmodifiable(tup.getTuple)
    override def arity = -1
  }
}
