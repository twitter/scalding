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
package com.twitter.scalding.typed.functions

import java.io.Serializable

/**
 * This is a composition of one or more FlatMappings
 *
 * For some reason, this fails in scala 2.12 if this is an abstract class
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
        val const: A1 => TraversableOnce[A1] = FlatMapFunctions.FromIdentity[A1]()
        type F[T] = A1 => TraversableOnce[T]
        ev.subst[F](const)
      case Single(Filter(f, ev)) =>
        val filter: A1 => TraversableOnce[A1] = FlatMapFunctions.FromFilter(f)
        type F[T] = A1 => TraversableOnce[T]
        ev.subst[F](filter)
      case Single(Map(f)) => FlatMapFunctions.FromMap(f)
      case Single(FlatM(f)) => f
      case Series(Identity(ev), rest) =>
        type F[T] = T => TraversableOnce[B1]
        ev.subst[F](loop(rest))
      case Series(Filter(f, ev), rest) =>
        type F[T] = T => TraversableOnce[B1]
        val next = ev.subst[F](loop(rest)) // linter:disable:UndesirableTypeInference

        FlatMapFunctions.FromFilterCompose(f, next)
      case Series(Map(f), rest) =>
        val next = loop(rest) // linter:disable:UndesirableTypeInference
        FlatMapFunctions.FromMapCompose(f, next)
      case Series(FlatM(f), rest) =>
        val next = loop(rest) // linter:disable:UndesirableTypeInference
        FlatMapFunctions.FromFlatMapCompose(f, next)
    }

    loop(this)
  }

  def apply(a: A): TraversableOnce[B] = toFn(a)
}

object FlatMappedFn {

  def asId[A, B](f: FlatMappedFn[A, B]): Option[EqTypes[_ >: A, _ <: B]] = f match {
    case Single(FlatMapping.Identity(ev)) => Some(ev)
    case _ => None
  }

  def asFilter[A, B](f: FlatMappedFn[A, B]): Option[(A => Boolean, EqTypes[(_ >: A), (_ <: B)])] = f match {
    case Single(filter@FlatMapping.Filter(_, _)) => Some((filter.fn, filter.ev))
    case _ => None
  }

  def apply[A, B](fn: A => TraversableOnce[B]): FlatMappedFn[A, B] =
    fn match {
      case fmf: FlatMappedFn[A, B] => fmf
      case rawfn => Single(FlatMapping.FlatM(rawfn))
    }

  def identity[T]: FlatMappedFn[T, T] = Single(FlatMapping.Identity[T, T](EqTypes.reflexive[T]))

  def fromFilter[A](fn: A => Boolean): FlatMappedFn[A, A] =
    Single(FlatMapping.Filter[A, A](fn, EqTypes.reflexive))

  def fromMap[A, B](fn: A => B): FlatMappedFn[A, B] =
    Single(FlatMapping.Map(fn))

  final case class Single[A, B](fn: FlatMapping[A, B]) extends FlatMappedFn[A, B]
  final case class Series[A, B, C](first: FlatMapping[A, B], next: FlatMappedFn[B, C]) extends FlatMappedFn[A, C]
}
