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
 * This is a more powerful version of =:= that can allow
 * us to remove casts and also not have any runtime cost
 * for our function calls in some cases of trivial functions
 */
sealed abstract class EqTypes[A, B] extends java.io.Serializable {
  def apply(a: A): B
  def subst[F[_]](f: F[A]): F[B]

  final def reverse: EqTypes[B, A] = {
    val aa = EqTypes.reflexive[A]
    type F[T] = EqTypes[T, A]
    subst[F](aa)
  }

  def toEv: A =:= B = {
    val aa = implicitly[A =:= A]
    type F[T] = A =:= T
    subst[F](aa)
  }
}

object EqTypes extends java.io.Serializable {
  private[this] final case class ReflexiveEquality[A]() extends EqTypes[A, A] {
    def apply(a: A): A = a
    def subst[F[_]](f: F[A]): F[A] = f
  }

  implicit def reflexive[A]: EqTypes[A, A] = ReflexiveEquality()
}

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
    filter[(K, V)](FlatMappedFn.FilterKeysToFilter(fn))

  final case class Identity[A, B](ev: EqTypes[A, B]) extends FlatMapping[A, B]
  final case class Filter[A, B](fn: A => Boolean, ev: EqTypes[A, B]) extends FlatMapping[A, B]
  final case class Map[A, B](fn: A => B) extends FlatMapping[A, B]
  final case class FlatM[A, B](fn: A => TraversableOnce[B]) extends FlatMapping[A, B]
}

/**
 * This is a composition of one or more FlatMappings
 */
sealed trait FlatMappedFn[-A, +B] extends (A => TraversableOnce[B]) with java.io.Serializable {
  import FlatMappedFn._

  final def runAfter[Z](fn: FlatMapping[Z, A]): FlatMappedFn[Z, B] = this match {
    case Single(FlatMapping.Identity(ev)) =>
      type F[T] = FlatMapping[Z, T]
      Single(ev.subst[F](fn))
    case notId => fn match {
      case FlatMapping.Identity(ev) =>
        type F[T] = FlatMappedFn[T, B]
        ev.reverse.subst[F](this)
      case notIdFn => Series(notIdFn, notId) // only make a Series without either side being identity
    }
  }

  final def combine[C](next: FlatMappedFn[B, C]): FlatMappedFn[A, C] = {
    /*
     * We have to reassociate so the front of the series has the
     * first flatmap, so we can bail out early when there are no more
     * items in any flatMap result.
     */
    def loop[X, Y, Z](fn0: FlatMappedFn[X, Y], fn1: FlatMappedFn[Y, Z]): FlatMappedFn[X, Z] =
      fn0 match {
        case Single(FlatMapping.Identity(ev)) =>
          type F[T] = FlatMappedFn[T, Z]
          ev.reverse.subst[F](fn1)
        case Single(f0) =>
          Series(f0, fn1)
        case Series(f0f, f1f) =>
          Series(f0f, loop(f1f, fn1))
      }
    loop(this, next)
  }

  /**
   * We interpret this composition once to minimize pattern matching when we execute
   */
  private[this] val toFn: A => TraversableOnce[B] = {
    import FlatMapping._

    def loop[A1, B1](fn: FlatMappedFn[A1, B1]): A1 => TraversableOnce[B1] = fn match {
      case Single(Identity(ev)) =>
        val const: A1 => TraversableOnce[A1] = FlatMappedFn.FromIdentity[A1]()
        type F[T] = A1 => TraversableOnce[T]
        ev.subst[F](const)
      case Single(Filter(f, ev)) =>
        val filter: A1 => TraversableOnce[A1] = FlatMappedFn.FromFilter(f)
        type F[T] = A1 => TraversableOnce[T]
        ev.subst[F](filter)
      case Single(Map(f)) => FlatMappedFn.FromMap(f)
      case Single(FlatM(f)) => f
      case Series(Identity(ev), rest) =>
        type F[T] = T => TraversableOnce[B1]
        ev.subst[F](loop(rest))
      case Series(Filter(f, ev), rest) =>
        type F[T] = T => TraversableOnce[B1]
        val next = ev.subst[F](loop(rest)) // linter:disable:UndesirableTypeInference

        FlatMappedFn.FromFilterCompose(f, next)
      case Series(Map(f), rest) =>
        val next = loop(rest) // linter:disable:UndesirableTypeInference
        FlatMappedFn.FromMapCompose(f, next)
      case Series(FlatM(f), rest) =>
        val next = loop(rest) // linter:disable:UndesirableTypeInference
        FlatMappedFn.FromFlatMapCompose(f, next)
    }

    loop(this)
  }

  def apply(a: A): TraversableOnce[B] = toFn(a)
}

object FlatMappedFn {

  /**
   * we prefer case class functions since they have equality
   */
  private case class FromIdentity[A]() extends Function1[A, Iterator[A]] {
    def apply(a: A) = Iterator.single(a)
  }
  private case class FromFilter[A](fn: A => Boolean) extends Function1[A, Iterator[A]] {
    def apply(a: A) = if (fn(a)) Iterator.single(a) else Iterator.empty
  }
  private case class FromMap[A, B](fn: A => B) extends Function1[A, Iterator[B]] {
    def apply(a: A) = Iterator.single(fn(a))
  }
  private case class FromFilterCompose[A, B](fn: A => Boolean, next: A => TraversableOnce[B]) extends Function1[A, TraversableOnce[B]] {
    def apply(a: A) = if (fn(a)) next(a) else Iterator.empty
  }
  private case class FromMapCompose[A, B, C](fn: A => B, next: B => TraversableOnce[C]) extends Function1[A, TraversableOnce[C]] {
    def apply(a: A) = next(fn(a))
  }
  private case class FromFlatMapCompose[A, B, C](fn: A => TraversableOnce[B], next: B => TraversableOnce[C]) extends Function1[A, TraversableOnce[C]] {
    def apply(a: A) = fn(a).flatMap(next)
  }


  import FlatMapping._

  def asId[A, B](f: FlatMappedFn[A, B]): Option[EqTypes[_ >: A, _ <: B]] = f match {
    case Single(Identity(ev)) => Some(ev)
    case _ => None
  }

  def asFilter[A, B](f: FlatMappedFn[A, B]): Option[(A => Boolean, EqTypes[(_ >: A), (_ <: B)])] = f match {
    case Single(filter@Filter(_, _)) => Some((filter.fn, filter.ev))
    case _ => None
  }

  def apply[A, B](fn: A => TraversableOnce[B]): FlatMappedFn[A, B] =
    fn match {
      case fmf: FlatMappedFn[A, B] => fmf
      case rawfn => Single(FlatMapping.FlatM(rawfn))
    }

  def identity[T]: FlatMappedFn[T, T] = Single(FlatMapping.Identity[T, T](EqTypes.reflexive[T]))

  case class FilterKeysToFilter[K](fn: K => Boolean) extends Function1[(K, Any), Boolean] {
    def apply(kv: (K, Any)) = fn(kv._1)
  }

  case class FlatMapValuesToFlatMap[K, A, B](fn: A => TraversableOnce[B]) extends Function1[(K, A), TraversableOnce[(K, B)]] {
    def apply(ka: (K, A)) = {
      val k = ka._1
      fn(ka._2).map((k, _))
    }
  }

  case class MapValuesToMap[K, A, B](fn: A => B) extends Function1[(K, A), (K, B)] {
    def apply(ka: (K, A)) = (ka._1, fn(ka._2))
  }

  def fromFilter[A](fn: A => Boolean): FlatMappedFn[A, A] =
    Single(FlatMapping.Filter[A, A](fn, EqTypes.reflexive))

  def fromMap[A, B](fn: A => B): FlatMappedFn[A, B] =
    Single(FlatMapping.Map(fn))

  final case class Single[A, B](fn: FlatMapping[A, B]) extends FlatMappedFn[A, B]
  final case class Series[A, B, C](first: FlatMapping[A, B], next: FlatMappedFn[B, C]) extends FlatMappedFn[A, C]
}
