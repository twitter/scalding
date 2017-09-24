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

import java.io.Serializable

import com.twitter.scalding.TupleConverter
import cascading.tuple.TupleEntry

/**
 * This is one of 4 core, non composed operations:
 * identity
 * filter
 * map
 * flatMap
 */
sealed trait FlatMapping[-A, +B] extends java.io.Serializable
object FlatMapping {
  def filter[A](fn: A => Boolean): FlatMapping[A, A] =
    Filter[A, A](fn, implicitly)

  def filterKeys[K, V](fn: K => Boolean): FlatMapping[(K, V), (K, V)] =
    filter { kv => fn(kv._1) }

  final case class Identity[A, B](ev: A =:= B) extends FlatMapping[A, B]
  final case class Filter[A, B](fn: A => Boolean, ev: A =:= B) extends FlatMapping[A, B]
  final case class Map[A, B](fn: A => B) extends FlatMapping[A, B]
  final case class FlatM[A, B](fn: A => TraversableOnce[B]) extends FlatMapping[A, B]
}

/**
 * This is a composition of one or more FlatMappings
 */
sealed trait FlatMappedFn[-A, +B] extends (A => TraversableOnce[B]) with java.io.Serializable {
  import FlatMappedFn._

  final def runAfter[Z](fn: FlatMapping[Z, A]): FlatMappedFn[Z, B] = this match {
    case Single(FlatMapping.Identity(_)) => Single(fn.asInstanceOf[FlatMapping[Z, B]]) // since we have A =:= B, we know this cast is safe
    case notId => fn match {
      case FlatMapping.Identity(ev) => this.asInstanceOf[FlatMappedFn[Z, B]] // we have Z =:= A we know this cast is safe
      case notIdFn => Series(notIdFn, notId) // only make a Series without either side being identity
    }
  }

  /**
   * We interpret this composition once to minimize pattern matching when we execute
   */
  private[this] val toFn: A => TraversableOnce[B] = {
    import FlatMapping._

    def loop[A1, B1](fn: FlatMappedFn[A1, B1]): A1 => TraversableOnce[B1] = fn match {
      case Single(Identity(ev)) =>
        { (t: A1) => Iterator.single(t.asInstanceOf[B1]) } // A1 =:= B1
      case Single(Filter(f, ev)) =>
        { (t: A1) => if (f(t)) Iterator.single(t.asInstanceOf[B1]) else Iterator.empty } // A1 =:= B1
      case Single(Map(f)) => f.andThen(Iterator.single)
      case Single(FlatM(f)) => f
      case Series(Identity(ev), rest) => loop(rest).asInstanceOf[A1 => TraversableOnce[B1]] // we know that A1 =:= C
      case Series(Filter(f, ev), rest) =>
        val next = loop(rest).asInstanceOf[A1 => TraversableOnce[B1]] // A1 =:= C

        { (t: A1) => if (f(t)) next(t) else Iterator.empty }
      case Series(Map(f), rest) =>
        val next = loop(rest) // linter:disable:UndesirableTypeInference
        f.andThen(next)
      case Series(FlatM(f), rest) =>
        val next = loop(rest) // linter:disable:UndesirableTypeInference
        f.andThen(_.flatMap(next))
    }

    loop(this)
  }

  def apply(a: A): TraversableOnce[B] = toFn(a)
}

object FlatMappedFn {
  import FlatMapping._

  def asId[A, B](f: FlatMappedFn[A, B]): Option[(_ >: A) =:= (_ <: B)] = f match {
    case Single(i@Identity(_)) => Some(i.ev)
    case _ => None
  }

  def asFilter[A, B](f: FlatMappedFn[A, B]): Option[(A => Boolean, (_ >: A) =:= (_ <: B))] = f match {
    case Single(filter@Filter(_, _)) => Some((filter.fn, filter.ev))
    case _ => None
  }

  def identity[T]: FlatMappedFn[T, T] = Single(FlatMapping.Identity[T, T](implicitly[T =:= T]))
  final case class Single[A, B](fn: FlatMapping[A, B]) extends FlatMappedFn[A, B]
  final case class Series[A, B, C](first: FlatMapping[A, B], next: FlatMappedFn[B, C]) extends FlatMappedFn[A, C]
}
