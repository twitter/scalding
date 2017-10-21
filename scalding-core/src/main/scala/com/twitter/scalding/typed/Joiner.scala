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

import com.twitter.scalding._

object Joiner extends java.io.Serializable {

  type JoinFn[K, V, U, R] = (K, Iterator[V], Iterable[U]) => Iterator[R]
  type HashJoinFn[K, V, U, R] = (K, V, Iterable[U]) => Iterator[R]

  def toCogroupJoiner2[K, V, U, R](hashJoiner: (K, V, Iterable[U]) => Iterator[R]): JoinFn[K, V, U, R] =
    JoinFromHashJoin(hashJoiner)

  def hashInner2[K, V, U]: HashJoinFn[K, V, U, (V, U)] =
    HashInner()

  def hashLeft2[K, V, U]: HashJoinFn[K, V, U, (V, Option[U])] =
    HashLeft()

  def inner2[K, V, U]: JoinFn[K, V, U, (V, U)] =
    InnerJoin()

  def asOuter[U](it: Iterator[U]): Iterator[Option[U]] =
    if (it.isEmpty) Iterator.single(None)
    else it.map(Some(_))

  def outer2[K, V, U]: JoinFn[K, V, U, (Option[V], Option[U])] =
    OuterJoin()

  def left2[K, V, U]: JoinFn[K, V, U, (V, Option[U])] =
    LeftJoin()

  def right2[K, V, U]: JoinFn[K, V, U, (Option[V], U)] =
    RightJoin()

  /**
   * Optimizers want to match on the kinds of joins we are doing.
   * This gives them that ability
   */
  sealed abstract class HashJoinFunction[K, V, U, R] extends Function3[K, V, Iterable[U], Iterator[R]]

  final case class HashInner[K, V, U]() extends HashJoinFunction[K, V, U, (V, U)] {
    def apply(k: K, v: V, u: Iterable[U]) = u.iterator.map((v, _))
  }
  final case class HashLeft[K, V, U]() extends HashJoinFunction[K, V, U, (V, Option[U])] {
    def apply(k: K, v: V, u: Iterable[U]) = asOuter(u.iterator).map((v, _))
  }
  final case class FilteredHashJoin[K, V1, V2, R](jf: HashJoinFunction[K, V1, V2, R], fn: ((K, R)) => Boolean) extends HashJoinFunction[K, V1, V2, R] {
    def apply(k: K, left: V1, right: Iterable[V2]) =
      jf.apply(k, left, right).filter { r => fn((k, r)) }
  }
  final case class MappedHashJoin[K, V1, V2, R, R1](jf: HashJoinFunction[K, V1, V2, R], fn: R => R1) extends HashJoinFunction[K, V1, V2, R1] {
    def apply(k: K, left: V1, right: Iterable[V2]) =
      jf.apply(k, left, right).map(fn)
  }
  final case class FlatMappedHashJoin[K, V1, V2, R, R1](jf: HashJoinFunction[K, V1, V2, R], fn: R => TraversableOnce[R1]) extends HashJoinFunction[K, V1, V2, R1] {
    def apply(k: K, left: V1, right: Iterable[V2]) =
      jf.apply(k, left, right).flatMap(fn)
  }

  sealed abstract class JoinFunction[K, V1, V2, R] extends Function3[K, Iterator[V1], Iterable[V2], Iterator[R]]

  final case class InnerJoin[K, V1, V2]() extends JoinFunction[K, V1, V2, (V1, V2)] {
    def apply(k: K, left: Iterator[V1], right: Iterable[V2]): Iterator[(V1, V2)] =
      left.flatMap { v1 => right.iterator.map((v1, _)) }
  }
  final case class LeftJoin[K, V1, V2]() extends JoinFunction[K, V1, V2, (V1, Option[V2])] {
    def apply(k: K, left: Iterator[V1], right: Iterable[V2]): Iterator[(V1, Option[V2])] =
      left.flatMap { v1 => asOuter(right.iterator).map((v1, _)) }
  }
  final case class RightJoin[K, V1, V2]() extends JoinFunction[K, V1, V2, (Option[V1], V2)] {
    def apply(k: K, left: Iterator[V1], right: Iterable[V2]): Iterator[(Option[V1], V2)] =
      asOuter(left).flatMap { v1 => right.iterator.map((v1, _)) }
  }
  final case class OuterJoin[K, V1, V2]() extends JoinFunction[K, V1, V2, (Option[V1], Option[V2])] {
    def apply(k: K, left: Iterator[V1], right: Iterable[V2]): Iterator[(Option[V1], Option[V2])] =
      if (left.isEmpty && right.isEmpty) Iterator.empty
      else asOuter(left).flatMap { v1 => asOuter(right.iterator).map((v1, _)) }
  }
  final case class FilteredJoin[K, V1, V2, R](jf: JoinFunction[K, V1, V2, R], fn: ((K, R)) => Boolean) extends JoinFunction[K, V1, V2, R] {
    def apply(k: K, left: Iterator[V1], right: Iterable[V2]) =
      jf.apply(k, left, right).filter { r => fn((k, r)) }
  }
  final case class MappedJoin[K, V1, V2, R, R1](jf: JoinFunction[K, V1, V2, R], fn: R => R1) extends JoinFunction[K, V1, V2, R1] {
    def apply(k: K, left: Iterator[V1], right: Iterable[V2]) =
      jf.apply(k, left, right).map(fn)
  }
  final case class FlatMappedJoin[K, V1, V2, R, R1](jf: JoinFunction[K, V1, V2, R], fn: R => TraversableOnce[R1]) extends JoinFunction[K, V1, V2, R1] {
    def apply(k: K, left: Iterator[V1], right: Iterable[V2]) =
      jf.apply(k, left, right).flatMap(fn)
  }
  final case class MappedGroupJoin[K, V1, V2, R, R1](jf: JoinFunction[K, V1, V2, R], fn: (K, Iterator[R]) => Iterator[R1]) extends JoinFunction[K, V1, V2, R1] {
    def apply(k: K, left: Iterator[V1], right: Iterable[V2]) = {
      val iterr = jf.apply(k, left, right)
      if (iterr.isEmpty) Iterator.empty // mapGroup operates on non-empty groups
      else fn(k, iterr)
    }
  }
  final case class JoinFromHashJoin[K, V1, V2, R](hj: (K, V1, Iterable[V2]) => Iterator[R]) extends JoinFunction[K, V1, V2, R] {
    def apply(k: K, itv: Iterator[V1], itu: Iterable[V2]) =
      itv.flatMap(hj(k, _, itu))
  }

  /**
   * an inner-like join function is empty definitely if either side is empty
   */
  final def isInnerJoinLike[K, V1, V2, R](jf: (K, Iterator[V1], Iterable[V2]) => Iterator[R]): Option[Boolean] =
    jf match {
      case InnerJoin() => Some(true)
      case LeftJoin() => Some(false)
      case RightJoin() => Some(false)
      case OuterJoin() => Some(false)
      case JoinFromHashJoin(hj) => isInnerHashJoinLike(hj)
      case FilteredJoin(jf, _) => isInnerJoinLike(jf)
      case MappedJoin(jf, _) => isInnerJoinLike(jf)
      case FlatMappedJoin(jf, _) => isInnerJoinLike(jf)
      case MappedGroupJoin(jf, _) => isInnerJoinLike(jf)
      case _ => None
    }
  /**
   * a left-like join function is empty definitely if the left side is empty
   */
  final def isLeftJoinLike[K, V1, V2, R](jf: (K, Iterator[V1], Iterable[V2]) => Iterator[R]): Option[Boolean] =
    jf match {
      case InnerJoin() => Some(true)
      case JoinFromHashJoin(hj) => isInnerHashJoinLike(hj)
      case LeftJoin() => Some(true)
      case RightJoin() => Some(false)
      case OuterJoin() => Some(false)
      case FilteredJoin(jf, _) => isLeftJoinLike(jf)
      case MappedJoin(jf, _) => isLeftJoinLike(jf)
      case FlatMappedJoin(jf, _) => isLeftJoinLike(jf)
      case MappedGroupJoin(jf, _) => isLeftJoinLike(jf)
      case _ => None
    }
  /**
   * a right-like join function is empty definitely if the right side is empty
   */
  final def isRightJoinLike[K, V1, V2, R](jf: (K, Iterator[V1], Iterable[V2]) => Iterator[R]): Option[Boolean] =
    jf match {
      case InnerJoin() => Some(true)
      case JoinFromHashJoin(hj) => isInnerHashJoinLike(hj)
      case LeftJoin() => Some(false)
      case RightJoin() => Some(true)
      case OuterJoin() => Some(false)
      case FilteredJoin(jf, _) => isRightJoinLike(jf)
      case MappedJoin(jf, _) => isRightJoinLike(jf)
      case FlatMappedJoin(jf, _) => isRightJoinLike(jf)
      case MappedGroupJoin(jf, _) => isRightJoinLike(jf)
      case _ => None
    }

  /**
   * a inner-like hash-join function is empty definitely if either side is empty
   */
  final def isInnerHashJoinLike[K, V1, V2, R](jf: (K, V1, Iterable[V2]) => Iterator[R]): Option[Boolean] =
    jf match {
      case HashInner() => Some(true)
      case HashLeft() => Some(false)
      case FilteredHashJoin(jf, _) => isInnerHashJoinLike(jf)
      case MappedHashJoin(jf, _) => isInnerHashJoinLike(jf)
      case FlatMappedHashJoin(jf, _) => isInnerHashJoinLike(jf)
      case _ => None
    }

  final case class CastingWideJoin[A]() extends Function3[Any, Iterator[Any], Seq[Iterable[Any]], Iterator[A]] {
    def apply(k: Any, iter: Iterator[Any], empties: Seq[Iterable[Any]]) = {
      assert(empties.isEmpty, "this join function should never be called with non-empty right-most")
      iter.asInstanceOf[Iterator[A]]
    }
  }
}

