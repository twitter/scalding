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

import cascading.tuple.{ Tuple => CTuple }

/**
 * Typeclass to represent converting back to (setting into) a cascading Tuple
 * This looks like it can be contravariant, but it can't because of our approach
 * of falling back to the singleSetter, you really want the most specific setter
 * you can get. Put more directly: a TupleSetter[Any] is not just as good as TupleSetter[(Int, Int)]
 * from the scalding DSL's point of view. The latter will flatten the (Int, Int), but the former
 * won't.
 */
trait TupleSetter[T] extends java.io.Serializable with TupleArity { self =>
  def apply(arg: T): CTuple

  def contraMap[U](fn: U => T): TupleSetter[U] =
    new TupleSetter[U] {
      def apply(arg: U) = self.apply(fn(arg))
      def arity = self.arity
    }
}

trait LowPriorityTupleSetters extends java.io.Serializable {
  /**
   * If it is not a scala Tuple, and not any defined in the object TupleSetter
   * we just assume it is a single entry in the tuple
   * For some reason, putting a val TupleSetter[Any] here messes up implicit resolution
   */
  implicit def singleSetter[A]: TupleSetter[A] = new TupleSetter[A] {
    override def apply(arg: A) = {
      val tup = CTuple.size(1)
      tup.set(0, arg)
      tup
    }
    override def arity = 1
  }
}

object TupleSetter extends GeneratedTupleSetters {

  /**
   * Treat this TupleSetter as one for a subclass
   * We do this because we want to use implicit resolution invariantly,
   * but clearly, the operation is contravariant
   */
  def asSubSetter[T, U <: T](ts: TupleSetter[T]): TupleSetter[U] = ts.asInstanceOf[TupleSetter[U]]

  def toCTuple[T](t: T)(implicit ts: TupleSetter[T]): CTuple = ts(t)
  def arity[T](implicit ts: TupleSetter[T]): Int = ts.arity
  def of[T](implicit ts: TupleSetter[T]): TupleSetter[T] = ts

  import shapeless.ops.hlist._
  import shapeless.ops.nat._
  import shapeless._

  implicit def hListSetter[L <: HList, N <: Nat, T <: Any]
    (implicit len: Length.Aux[L, N], ti: ToInt[N], tl: ToList[L, T]) = new TupleSetter[L] {

        override def apply(arg: L): CTuple = {
          val list = arg.toList[T].map(_.asInstanceOf[Object])
          new CTuple(list:_*)
        }

        override def arity: Int = ti()
      }

  //This is here for handling functions that return cascading tuples:
  implicit lazy val CTupleSetter: TupleSetter[CTuple] = new TupleSetter[CTuple] {
    override def apply(arg: CTuple) = new CTuple(arg)
    //We return an invalid value here, so we must check returns
    override def arity = -1
  }

  //Unit is like a Tuple0. It corresponds to Tuple.NULL
  implicit lazy val UnitSetter: TupleSetter[Unit] = new TupleSetter[Unit] {
    override def apply(arg: Unit) = CTuple.NULL
    override def arity = 0
  }

  // Doesn't seem safe to make this implicit by default:
  lazy val ProductSetter: TupleSetter[Product] = new TupleSetter[Product] {
    def apply(in: Product) = {
      val t = new CTuple
      in.productIterator.foreach(t.add(_))
      t
    }
    def arity = -1
  }
}
