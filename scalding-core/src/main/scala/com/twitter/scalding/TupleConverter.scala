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

import cascading.tuple.TupleEntry
import cascading.tuple.{ Tuple => CTuple }

import scala.collection.breakOut

/**
 * Typeclass to represent converting from cascading TupleEntry to some type T.
 * The most common application is to convert to scala Tuple objects for use
 * with the Fields API.  The typed API internally manually handles its mapping
 * to cascading Tuples, so the implicit resolution mechanism is not used.
 *
 * WARNING: if you are seeing issues with the singleConverter being found when you
 * expect something else, you may have an issue where the enclosing scope needs to
 * take an implicit TupleConverter of the correct type.
 *
 * Unfortunately, the semantics we want (prefer to flatten tuples, but otherwise
 * put everything into one postition in the tuple) are somewhat difficlut to
 * encode in scala.
 */
trait TupleConverter[@specialized(Int, Long, Float, Double) T] extends java.io.Serializable with TupleArity { self =>
  def apply(te: TupleEntry): T
  def andThen[U](fn: T => U): TupleConverter[U] = new TupleConverter[U] {
    def apply(te: TupleEntry) = fn(self(te))
    def arity = self.arity
  }
}

trait LowPriorityTupleConverters extends java.io.Serializable {
  implicit def singleConverter[@specialized(Int, Long, Float, Double) A](implicit g: TupleGetter[A]): TupleConverter[A] =
    new TupleConverter[A] {
      def apply(tup: TupleEntry) = g.get(tup.getTuple, 0)
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

  def build[T](thisArity: Int)(fn: TupleEntry => T): TupleConverter[T] = new TupleConverter[T] {
    def apply(te: TupleEntry) = fn(te)
    def arity = thisArity
  }
  def fromTupleEntry[T](t: TupleEntry)(implicit tc: TupleConverter[T]): T = tc(t)
  def arity[T](implicit tc: TupleConverter[T]): Int = tc.arity
  def of[T](implicit tc: TupleConverter[T]): TupleConverter[T] = tc

  /**
   * Copies the tupleEntry, since cascading may change it after the end of an
   * operation (and it is not safe to assume the consumer has not kept a ref
   * to this tuple)
   */
  implicit lazy val TupleEntryConverter: TupleConverter[TupleEntry] = new TupleConverter[TupleEntry] {
    override def apply(tup: TupleEntry) = new TupleEntry(tup)
    override def arity = -1
  }

  /**
   * Copies the tuple, since cascading may change it after the end of an
   * operation (and it is not safe to assume the consumer has not kept a ref
   * to this tuple
   */
  implicit lazy val CTupleConverter: TupleConverter[CTuple] = new TupleConverter[CTuple] {
    override def apply(tup: TupleEntry) = tup.getTupleCopy
    override def arity = -1
  }

  /**
   * In the case where you don't know the arity, prefer to use this.
   */
  implicit lazy val ProductTupleConverter: TupleConverter[Product] = new TupleConverter[Product] {
    def wrap(tup: CTuple): Product = new Product {
      def canEqual(that: Any) = that match {
        case p: Product => true
        case _ => false
      }
      def productArity = tup.size
      def productElement(idx: Int) = tup.getObject(idx)
    }
    override def apply(tup: TupleEntry) = wrap(tup.getTupleCopy)
    override def arity = -1
  }

  implicit lazy val UnitConverter: TupleConverter[Unit] = new TupleConverter[Unit] {
    override def apply(arg: TupleEntry) = ()
    override def arity = 0
  }
  // Doesn't seem safe to make these implicit by default:
  /**
   * Convert a TupleEntry to a List of CTuple, of length 2, with key, value
   * from the TupleEntry (useful for RichPipe.unpivot)
   */
  object KeyValueList extends TupleConverter[List[CTuple]] {
    def apply(tupe: TupleEntry): List[CTuple] = {
      val keys = tupe.getFields
      (0 until keys.size).map { idx =>
        new CTuple(keys.get(idx).asInstanceOf[Object], tupe.getObject(idx))
      }(breakOut)
    }
    def arity = -1
  }

  object ToMap extends TupleConverter[Map[String, AnyRef]] {
    def apply(tupe: TupleEntry): Map[String, AnyRef] = {
      val keys = tupe.getFields
      (0 until keys.size).map { idx =>
        (keys.get(idx).toString, tupe.getObject(idx))
      }(breakOut)
    }
    def arity = -1
  }

  /**
   * Utility to create a single item Tuple
   */
  def tupleAt(idx: Int)(tup: CTuple): CTuple = {
    val obj = tup.getObject(idx)
    val res = CTuple.size(1)
    res.set(0, obj)
    res
  }
}
