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

  case class Identity[A, B](ev: A =:= B) extends FlatMapping[A, B]
  case class Filter[A, B](fn: A => Boolean, ev: A =:= B) extends FlatMapping[A, B]
  case class Map[A, B](fn: A => B) extends FlatMapping[A, B]
  case class FlatM[A, B](fn: A => TraversableOnce[B]) extends FlatMapping[A, B]
}

/**
 * This is a composition of one or more FlatMappings
 */
sealed trait FlatMappedFn[-A, +B] extends (A => TraversableOnce[B]) with java.io.Serializable {
  import FlatMappedFn._

  final def compose[Z](fn: FlatMapping[Z, A]): FlatMappedFn[Z, B] = this match {
    case Single(FlatMapping.Identity(_)) => Single(fn.asInstanceOf[FlatMapping[Z, B]]) // since we have A =:= B, we know this cast is safe
    case notId => fn match {
      case FlatMapping.Identity(ev) => this.asInstanceOf[FlatMappedFn[Z, B]] // we have Z =:= A we know this cast is safe
      case notIdFn => Series(notIdFn, notId) // only make a Series without either side being identity
    }
  }

  final def apply(a: A): TraversableOnce[B] = {
    import FlatMapping._

    @annotation.tailrec
    def loop(t: Any, fn: FlatMappedFn[Any, B]): TraversableOnce[B] = fn match {
      case Single(Identity(ev)) => Iterator.single(ev(t))
      case Single(Filter(f, ev)) => if (f(t)) Iterator.single(ev(t)) else Iterator.empty
      case Single(Map(f)) => Iterator.single(f(t))
      case Single(FlatM(f)) => f(t)
      case Series(Identity(ev), rest) => loop(ev(t), rest)
      case Series(Filter(f, ev), rest) => if (f(t)) loop(ev(t), rest) else Iterator.empty
      case Series(Map(f), rest) => loop(f(t), rest)
      case Series(FlatM(f), rest) => f(t).flatMap(cheat(_, rest))
    }

    // here to cheat on tailrec
    def cheat(a: Any, f: FlatMappedFn[Any, B]): TraversableOnce[B] =
      loop(a, f)

    // the cast is so we can get a tailrec loop (which can't deal with changing types
    loop(a, this.asInstanceOf[FlatMappedFn[Any, B]])
  }
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
  case class Single[A, B](fn: FlatMapping[A, B]) extends FlatMappedFn[A, B]
  case class Series[A, B, C](first: FlatMapping[A, B], next: FlatMappedFn[B, C]) extends FlatMappedFn[A, C]
}
