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

import cascading.tuple.{Tuple => CTuple, Fields}
import cascading.pipe.joiner.{Joiner => CJoiner, JoinerClosure}

import cascading.pipe.{CoGroup, Pipe}

import com.twitter.scalding._

import scala.collection.JavaConverters._

trait CoGrouped[+K,+R] extends KeyedListLike[K,R,CoGrouped] {
  def inputs: List[ReduceStep[K,_,_]]
  /**
   * This function is not type-safe for others to call, but it should
   * never have an error. By construction, we never call it with incorrect
   * types.
   * It would be preferable to have stronger type safety here, but unclear
   * how to achieve, and since it is an internal function, not clear it
   * would actually help anyone for it to be type-safe
   */
  protected def joinFunction: (Any, Iterator[CTuple], Seq[Iterable[CTuple]]) => Iterator[R]

  override def mapValueStream[R1](fn: Iterator[R] => Iterator[R1]): CoGrouped[K, R1] = {
    val self = this // the usual self => trick leads to serialization errors
    new CoGrouped[K, R1] {
      def inputs = self.inputs
      def reducers = self.reducers
      def joinFunction = { (k: Any, leftMost: Iterator[CTuple], joins: Seq[Iterable[CTuple]]) =>
        fn(self.joinFunction(k, leftMost, joins))
      }
    }
  }

  /**
   * Smaller is about average values/key not total size (that does not matter, but is
   * clearly related).
   *
   * Note that from the type signature we see that the right side is iterated (or may be)
   * over and over, but the left side is not. That means that you want the side with
   * fewer values per key on the right. If both sides are similar, no need to worry.
   * If one side is a one-to-one mapping, that should be the "smaller" side.
   */
  def cogroup[K1>:K,R1,R2](smaller: CoGrouped[K1, R1])(fn: (K1, Iterator[R], Iterable[R1]) => Iterator[R2]): CoGrouped[K1, R2] = {
    val self = this
    val leftSeqCount = self.inputs.size - 1

    new CoGrouped[K1, R2] {
      val inputs = self.inputs ++ smaller.inputs
      val reducers = (self.reducers.toIterable ++ smaller.reducers.toIterable).reduceOption(_ max _)

      def joinFunction = { (k: Any, leftMost: Iterator[CTuple], joins: Seq[Iterable[CTuple]]) =>
        val joinedLeft = self.joinFunction(k, leftMost, joins.take(leftSeqCount))

        val smallerIns = joins.drop(leftSeqCount)
        val joinedRight = new Iterable[R1] {
          def iterator = smaller.joinFunction(k, smallerIns.head.iterator, smallerIns.tail)
        }

        fn(k.asInstanceOf[K1], joinedLeft, joinedRight)
      }
    }
  }

  def join[K1>:K,W](smaller: CoGrouped[K1,W]) =
    cogroup[K1,W,(R,W)](smaller)(Joiner.inner2)
  def leftJoin[K1>:K,W](smaller: CoGrouped[K1,W]) =
    cogroup[K1,W,(R,Option[W])](smaller)(Joiner.left2)
  def rightJoin[K1>:K,W](smaller: CoGrouped[K1,W]) =
    cogroup[K1,W,(Option[R],W)](smaller)(Joiner.right2)
  def outerJoin[K1>:K,W](smaller: CoGrouped[K1,W]) =
    cogroup[K1,W,(Option[R],Option[W])](smaller)(Joiner.outer2)
  // TODO: implement blockJoin

  override lazy val toTypedPipe: TypedPipe[(K, R)] = {
    inputs.foreach { step =>
      assert(step.valueOrdering == None, "secondary sorting unsupported in CoGrouped")
    }

    // Cascading handles the first item in join differently, we have to see if it is repeated
    val firstCount = inputs.map(_.mapped).count(_ == inputs.head.mapped)

    import Dsl._
    import RichPipe.assignName

    // It is important to use the right most ordering since that is possibly a superclass of the other Ks
    val rightMostOrdering = inputs.last.keyOrdering

    /*
     * we only want key and value.
     * Cascading requires you have the same number coming in as out.
     * in the first case, we introduce (null0, null1), in the second
     * we have (key1, value1), but they are then discarded:
     */
    def outFields(inCount: Int): Fields =
      List("key", "value") ++ (0 until (2*(inCount - 1))).map("null%d".format(_))

    val newPipe = if(firstCount == inputs.size) {
      // This is a self-join
      val NUM_OF_SELF_JOINS = firstCount - 1
      new CoGroup(assignName(inputs.head.mappedPipe),
        RichFields(StringField("key")(rightMostOrdering, None)),
        NUM_OF_SELF_JOINS,
        outFields(firstCount),
        new DistinctCoGroupJoiner(firstCount, joinFunction))
    }
    else if(firstCount == 1) {
      // As long as the first one appears only once, we can handle self joins on the others:
      def renamePipe(idx: Int, p: Pipe): Pipe =
        p.rename((List("key", "value") -> List("key%d".format(idx), "value%d".format(idx))))

      // distinct by mapped, but don't reorder if the list is unique
      @annotation.tailrec
      def distinctBy[T,U](list: List[T], acc: List[T] = Nil)(fn: T => U): List[T] = list match {
        case Nil => acc.reverse
        case h::tail if(acc.contains(h)) => distinctBy(tail, acc)(fn)
        case h::tail => distinctBy(tail, h::acc)(fn)
      }

      val distincts = distinctBy(inputs)(_.mapped)
      val dsize = distincts.size
      val isize = inputs.size

      val groupFields: Array[Fields] = (0 until dsize)
        .map { idx => RichFields(StringField("key%d".format(idx))(rightMostOrdering, None)) }
        .toArray

      val pipes: Array[Pipe] = distincts
        .zipWithIndex
        .map { case (item, idx) => assignName(renamePipe(idx, item.mappedPipe)) }
        .toArray

      val cjoiner = if(isize != dsize) {
        // avoid capturing anything other than the mapping ints:
        val mapping: Map[Int, Int] = inputs.zipWithIndex.map { case (item, idx) =>
          idx -> distincts.indexWhere(_.mapped == item.mapped)
        }.toMap

        new CoGroupedJoiner(isize, joinFunction) {
          val distinctSize = dsize
          def distinctIndexOf(orig: Int) = mapping(orig)
        }
      }
      else new DistinctCoGroupJoiner(isize, joinFunction)

      new CoGroup(pipes, groupFields, outFields(dsize), cjoiner)
    }
    else {
      sys.error("Except for self joins, where you are joining something with only itself,\n" +
        "left-most pipe can only appear once.")
    }
    /*
     * the CoGrouped only populates the first two fields, the second two
     * are null. We then project out at the end of the method.
     */
    val pipeWithRed = RichPipe.setReducers(newPipe, reducers.getOrElse(-1)).project('key, 'value)
    //Construct the new TypedPipe
    TypedPipe.from[(K,R)](pipeWithRed, ('key, 'value))
  }

}

abstract class CoGroupedJoiner(inputSize: Int, joinFunction: (Any, Iterator[CTuple], Seq[Iterable[CTuple]]) => Iterator[Any]) extends CJoiner {
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
    val key = iters
      .collectFirst { case iter if iter.nonEmpty => iter.head }
      .get // One of these must have a key
      .getObject(0)

    val leftMost = iters.head

    def toIterable(didx: Int) =
      new Iterable[CTuple] { def iterator = jc.getIterator(didx).asScala }

    val rest = restIndices.map(toIterable(_))
    joinFunction(key, leftMost, rest).map { rval =>
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
class DistinctCoGroupJoiner(count: Int,
  joinFunction: (Any, Iterator[CTuple], Seq[Iterable[CTuple]]) => Iterator[Any])
  extends CoGroupedJoiner(count, joinFunction) {
  val distinctSize = count
  def distinctIndexOf(idx: Int) = idx
}
