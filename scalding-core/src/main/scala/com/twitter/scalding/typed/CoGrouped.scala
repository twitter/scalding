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

import cascading.tuple.{ Tuple => CTuple, Fields }
import cascading.pipe.joiner.{ Joiner => CJoiner, JoinerClosure }

import cascading.pipe.{ CoGroup, Pipe }

import com.twitter.scalding._

import scala.annotation.meta.param
import scala.collection.JavaConverters._

import com.twitter.scalding.serialization.Externalizer
import com.twitter.scalding.TupleConverter.tuple2Converter
import com.twitter.scalding.TupleSetter.tup2Setter

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
  protected def joinFunction: (K, Iterator[CTuple], Seq[Iterable[CTuple]]) => Iterator[R]

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
      val reducers = (self.reducers.toIterable ++ smaller.reducers.toIterable).reduceOption(_ max _)
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

trait CoGrouped[K, +R] extends KeyedListLike[K, R, CoGrouped] with CoGroupable[K, R] with WithReducers[CoGrouped[K, R]] with WithDescription[CoGrouped[K, R]] {
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

  override lazy val toTypedPipe: TypedPipe[(K, R)] = {
    // Cascading handles the first item in join differently, we have to see if it is repeated
    val firstCount = inputs.count(_ == inputs.head)

    import Dsl._
    import RichPipe.assignName

    /*
     * we only want key and value.
     * Cascading requires you have the same number coming in as out.
     * in the first case, we introduce (null0, null1), in the second
     * we have (key1, value1), but they are then discarded:
     */
    def outFields(inCount: Int): Fields =
      List("key", "value") ++ (0 until (2 * (inCount - 1))).map("null%d".format(_))

    // Make this stable so the compiler does not make a closure
    val ord = keyOrdering

    TypedPipeFactory({ (flowDef, mode) =>
      val newPipe = Grouped.maybeBox[K, Any](ord, flowDef) { (tupset, ordKeyField) =>
        if (firstCount == inputs.size) {
          /**
           * This is a self-join
           * Cascading handles this by sending the data only once, spilling to disk if
           * the groups don't fit in RAM, then doing the join on this one set of data.
           * This is fundamentally different than the case where the first item is
           * not repeated. That case is below
           */
          val NUM_OF_SELF_JOINS = firstCount - 1
          new CoGroup(assignName(inputs.head.toPipe[(K, Any)](("key", "value"))(flowDef, mode,
            tupset)),
            ordKeyField,
            NUM_OF_SELF_JOINS,
            outFields(firstCount),
            WrappedJoiner(new DistinctCoGroupJoiner(firstCount, Grouped.keyGetter(ord), joinFunction)))
        } else if (firstCount == 1) {

          def keyId(idx: Int): String = "key%d".format(idx)
          /**
           * As long as the first one appears only once, we can handle self joins on the others:
           * Cascading does this by maybe spilling all the streams other than the first item.
           * This is handled by a different CoGroup constructor than the above case.
           */
          def renamePipe(idx: Int, p: TypedPipe[(K, Any)]): Pipe =
            p.toPipe[(K, Any)](List(keyId(idx), "value%d".format(idx)))(flowDef, mode,
              tupset)

          // This is tested for the properties we need (non-reordering)
          val distincts = CoGrouped.distinctBy(inputs)(identity)
          val dsize = distincts.size
          val isize = inputs.size

          def makeFields(id: Int): Fields = {
            val comp = ordKeyField.getComparators.apply(0)
            val fieldName = keyId(id)
            val f = new Fields(fieldName)
            f.setComparator(fieldName, comp)
            f
          }

          val groupFields: Array[Fields] = (0 until dsize)
            .map(makeFields)
            .toArray

          val pipes: Array[Pipe] = distincts
            .zipWithIndex
            .map { case (item, idx) => assignName(renamePipe(idx, item)) }
            .toArray

          val cjoiner = if (isize != dsize) {
            // avoid capturing anything other than the mapping ints:
            val mapping: Map[Int, Int] = inputs.zipWithIndex.map {
              case (item, idx) =>
                idx -> distincts.indexWhere(_ == item)
            }.toMap

            new CoGroupedJoiner(isize, Grouped.keyGetter(ord), joinFunction) {
              val distinctSize = dsize
              def distinctIndexOf(orig: Int) = mapping(orig)
            }
          } else {
            new DistinctCoGroupJoiner(isize, Grouped.keyGetter(ord), joinFunction)
          }

          new CoGroup(pipes, groupFields, outFields(dsize), WrappedJoiner(cjoiner))
        } else {
          /**
           * This is non-trivial to encode in the type system, so we throw this exception
           * at the planning phase.
           */
          sys.error("Except for self joins, where you are joining something with only itself,\n" +
            "left-most pipe can only appear once. Firsts: " +
            inputs.collect { case x if x == inputs.head => x }.toString)
        }
      }
      /*
       * the CoGrouped only populates the first two fields, the second two
       * are null. We then project out at the end of the method.
       */
      val pipeWithRedAndDescriptions = {
        RichPipe.setReducers(newPipe, reducers.getOrElse(-1))
        RichPipe.setPipeDescriptions(newPipe, descriptions)
        newPipe.project('key, 'value)
      }
      //Construct the new TypedPipe
      TypedPipe.from[(K, R)](pipeWithRedAndDescriptions, ('key, 'value))(flowDef, mode, tuple2Converter)
    })
  }
}

abstract class CoGroupedJoiner[K](inputSize: Int,
  getter: TupleGetter[K],
  @(transient @param) inJoinFunction: (K, Iterator[CTuple], Seq[Iterable[CTuple]]) => Iterator[Any]) extends CJoiner {

  /**
   * We have a test that should fail if Externalizer is not used here.
   * you can test failure of that test by replacing Externalizer with Some
   */
  val joinFunction = Externalizer(inJoinFunction)
  val distinctSize: Int
  def distinctIndexOf(originalPos: Int): Int

  // This never changes. Compute it once
  protected val restIndices: IndexedSeq[Int] = (1 until inputSize).map { idx =>
    val didx = distinctIndexOf(idx)
    assert(didx > 0, "the left most can only be iterated once")
    didx
  }

  override def getIterator(jc: JoinerClosure) = {
    val iters = (0 until distinctSize).map { jc.getIterator(_).asScala.buffered }
    val keyTuple = iters
      .collectFirst { case iter if iter.nonEmpty => iter.head }
      .get // One of these must have a key
    val key = getter.get(keyTuple, 0)

    val leftMost = iters.head

    def toIterable(didx: Int) =
      new Iterable[CTuple] { def iterator = jc.getIterator(didx).asScala }

    val rest = restIndices.map(toIterable(_))
    joinFunction.get(key, leftMost, rest).map { rval =>
      // There always has to be the same number of resulting fields as input
      // or otherwise the flow planner will throw
      val res = CTuple.size(distinctSize)
      res.set(0, key)
      res.set(1, rval)
      res
    }.asJava
  }

  override def numJoins = distinctSize - 1
}

// If all the input pipes are unique, this works:
class DistinctCoGroupJoiner[K](count: Int,
  getter: TupleGetter[K],
  @(transient @param) joinF: (K, Iterator[CTuple], Seq[Iterable[CTuple]]) => Iterator[Any])
  extends CoGroupedJoiner[K](count, getter, joinF) {
  val distinctSize = count
  def distinctIndexOf(idx: Int) = idx
}
