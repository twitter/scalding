/*
Copyright 2014 Twitter, Inc.

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

import cascading.tuple.{ Tuple => CTuple }

object CoGrouped {
  // distinct by mapped, but don't reorder if the list is unique
  final def distinctBy[T, U](list: List[T])(fn: T => U): List[T] = {
    @annotation.tailrec
    def go(l: List[T], seen: Set[U] = Set[U](), acc: List[T] = Nil): List[T] = l match {
      case Nil => acc.reverse // done
      case h :: tail =>
        val uh = fn(h)
        if (seen(uh))
          go(tail, seen, acc)
        else
          go(tail, seen + uh, h :: acc)
    }
    go(list)
  }
}

object CoGroupable {
  /*
   * This is the default empty join function needed for CoGroupable and HashJoinable
   */
  def castingJoinFunction[V]: (Any, Iterator[CTuple], Seq[Iterable[CTuple]]) => Iterator[V] =
    { (k, iter, empties) =>
      assert(empties.isEmpty, "this join function should never be called with non-empty right-most")
      iter.map(_.getObject(Grouped.ValuePosition).asInstanceOf[V])
    }
}

/**
 * Represents something than can be CoGrouped with another CoGroupable
 */
trait CoGroupable[K, +R] extends HasReducers with HasDescription with java.io.Serializable {
  /**
   * This is the list of mapped pipes, just before the (reducing) joinFunction is applied
   */
  def inputs: List[TypedPipe[(K, Any)]]

  def keyOrdering: Ordering[K]

  /**
   * This function is not type-safe for others to call, but it should
   * never have an error. By construction, we never call it with incorrect
   * types.
   * It would be preferable to have stronger type safety here, but unclear
   * how to achieve, and since it is an internal function, not clear it
   * would actually help anyone for it to be type-safe
   */
  private[scalding] def joinFunction: (K, Iterator[CTuple], Seq[Iterable[CTuple]]) => Iterator[R]

  /**
   * Smaller is about average values/key not total size (that does not matter, but is
   * clearly related).
   *
   * Note that from the type signature we see that the right side is iterated (or may be)
   * over and over, but the left side is not. That means that you want the side with
   * fewer values per key on the right. If both sides are similar, no need to worry.
   * If one side is a one-to-one mapping, that should be the "smaller" side.
   */
  def cogroup[R1, R2](smaller: CoGroupable[K, R1])(fn: (K, Iterator[R], Iterable[R1]) => Iterator[R2]): CoGrouped[K, R2] = {
    val self = this
    val leftSeqCount = self.inputs.size - 1
    val jf = joinFunction // avoid capturing `this` in the closure below
    val smallerJf = smaller.joinFunction

    new CoGrouped[K, R2] {
      val inputs = self.inputs ++ smaller.inputs
      val reducers = (self.reducers.iterator ++ smaller.reducers.iterator).reduceOption(_ max _)
      val descriptions: Seq[String] = self.descriptions ++ smaller.descriptions
      def keyOrdering = smaller.keyOrdering

      /**
       * Avoid capturing anything below as it will need to be serialized and sent to
       * all the reducers.
       */
      def joinFunction = { (k: K, leftMost: Iterator[CTuple], joins: Seq[Iterable[CTuple]]) =>
        val (leftSeq, rightSeq) = joins.splitAt(leftSeqCount)
        val joinedLeft = jf(k, leftMost, leftSeq)

        // Only do this once, for all calls to iterator below
        val smallerHead = rightSeq.head
        val smallerTail = rightSeq.tail
        // TODO: it might make sense to cache this in memory as an IndexedSeq and not
        // recompute it on every value for the left if the smallerJf is non-trivial
        // we could see how long it is, and possible switch to a cached version the
        // second time through if it is small enough
        val joinedRight = new Iterable[R1] {
          def iterator = smallerJf(k, smallerHead.iterator, smallerTail)
        }

        fn(k, joinedLeft, joinedRight)
      }
    }
  }

  def join[W](smaller: CoGroupable[K, W]) =
    cogroup[W, (R, W)](smaller)(Joiner.inner2)
  def leftJoin[W](smaller: CoGroupable[K, W]) =
    cogroup[W, (R, Option[W])](smaller)(Joiner.left2)
  def rightJoin[W](smaller: CoGroupable[K, W]) =
    cogroup[W, (Option[R], W)](smaller)(Joiner.right2)
  def outerJoin[W](smaller: CoGroupable[K, W]) =
    cogroup[W, (Option[R], Option[W])](smaller)(Joiner.outer2)
  // TODO: implement blockJoin
}

trait CoGrouped[K, +R] extends KeyedListLike[K, R, CoGrouped] with CoGroupable[K, R] with WithReducers[CoGrouped[K, R]] with WithDescription[CoGrouped[K, R]] with java.io.Serializable {
  override def withReducers(reds: Int) = {
    val self = this // the usual self => trick leads to serialization errors
    val joinF = joinFunction // can't access this on self, since it is protected
    new CoGrouped[K, R] {
      def inputs = self.inputs
      def reducers = Some(reds)
      def keyOrdering = self.keyOrdering
      def joinFunction = joinF
      def descriptions: Seq[String] = self.descriptions
    }
  }

  override def withDescription(description: String) = {
    val self = this // the usual self => trick leads to serialization errors
    val joinF = joinFunction // can't access this on self, since it is protected
    new CoGrouped[K, R] {
      def inputs = self.inputs
      def reducers = self.reducers
      def keyOrdering = self.keyOrdering
      def joinFunction = joinF
      def descriptions: Seq[String] = self.descriptions :+ description
    }
  }

  /**
   * It seems complex to push a take up to the mappers before a general join.
   * For some cases (inner join), we could take at most n from each TypedPipe,
   * but it is not clear how to generalize that for general cogrouping functions.
   * For now, just do a normal take.
   */
  override def bufferedTake(n: Int): CoGrouped[K, R] =
    take(n)

  // Filter the keys before doing the join
  override def filterKeys(fn: K => Boolean): CoGrouped[K, R] = {
    val self = this // the usual self => trick leads to serialization errors
    val joinF = joinFunction // can't access this on self, since it is protected
    new CoGrouped[K, R] {
      val inputs = self.inputs.map(_.filterKeys(fn))
      def reducers = self.reducers
      def descriptions: Seq[String] = self.descriptions
      def keyOrdering = self.keyOrdering
      def joinFunction = joinF
    }
  }

  override def mapGroup[R1](fn: (K, Iterator[R]) => Iterator[R1]): CoGrouped[K, R1] = {
    val self = this // the usual self => trick leads to serialization errors
    val joinF = joinFunction // can't access this on self, since it is protected
    new CoGrouped[K, R1] {
      def inputs = self.inputs
      def reducers = self.reducers
      def descriptions: Seq[String] = self.descriptions
      def keyOrdering = self.keyOrdering
      def joinFunction = { (k: K, leftMost: Iterator[CTuple], joins: Seq[Iterable[CTuple]]) =>
        val joined = joinF(k, leftMost, joins)
        /*
         * After the join, if the key has no values, don't present it to the mapGroup
         * function. Doing so would break the invariant:
         *
         * a.join(b).toTypedPipe.group.mapGroup(fn) == a.join(b).mapGroup(fn)
         */
        Grouped.addEmptyGuard(fn)(k, joined)
      }
    }
  }

  override def toTypedPipe: TypedPipe[(K, R)] =
    TypedPipe.CoGroupedPipe(this)
}
