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

import cascading.tuple.{Tuple => CTuple, TupleEntry}

/** A Type-class that setting TO cascading Tuple objects
 */

//TupleSetter[AnyRef] <: TupleSetter[String] so TupleSetter is contravariant
abstract class TupleSetter[-T] extends java.io.Serializable with TupleArity {
  def apply(arg : T) : CTuple
}

trait LowPriorityTupleSetters extends java.io.Serializable {
  // If it is not a scala Tuple, and not any defined in the object TupleSetter
  // we just assume it is a single entry in the tuple
  // For some reason, putting a val TupleSetter[Any] here messes up implicit resolution
  implicit def single[T]: TupleSetter[T] = new TupleSetter[T] {
    override def apply(arg : T) = {
      val tup = new CTuple
      tup.add(arg)
      tup
    }
    override def arity = 1
  }
}

object TupleSetter extends GeneratedTupleSetters {
  def set[T](t: T)(implicit ts: TupleSetter[T]): CTuple = ts(t)
  def arity[T](implicit ts: TupleSetter[T]): Int = ts.arity
  def of[T](implicit ts: TupleSetter[T]): TupleSetter[T] = ts

  //This is here for handling functions that return cascading tuples:
  implicit val ctuple: TupleSetter[CTuple] = new TupleSetter[CTuple] {
    override def apply(arg : CTuple) = arg
    //We return an invalid value here, so we must check returns
    override def arity = -1
  }

  //Unit is like a Tuple0. It corresponds to Tuple.NULL
  implicit val unit: TupleSetter[Unit] = new TupleSetter[Unit] {
    override def apply(arg : Unit) = CTuple.NULL
    override def arity = 0
  }
}

