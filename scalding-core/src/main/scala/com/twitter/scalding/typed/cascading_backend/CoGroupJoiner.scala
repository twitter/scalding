package com.twitter.scalding.typed.cascading_backend

import cascading.pipe.joiner.{ Joiner => CJoiner, JoinerClosure }
import cascading.tuple.{ Tuple => CTuple }
import com.twitter.scalding.{ TupleGetter }
import com.twitter.scalding.serialization.Externalizer
import scala.collection.JavaConverters._

abstract class CoGroupedJoiner[K](inputSize: Int,
  getter: TupleGetter[K],
  @transient inJoinFunction: (K, Iterator[Any], Seq[Iterable[Any]]) => Iterator[Any]) extends CJoiner {

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
    // This use of `_.get` is safe, but difficult to prove in the types.
    @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
    val keyTuple = iters
      .collectFirst { case iter if iter.nonEmpty => iter.head }
      .get // One of these must have a key
    val key = getter.get(keyTuple, 0)

    def unbox(it: Iterator[CTuple]): Iterator[Any] =
      it.map(_.getObject(1): Any)

    val leftMost = unbox(iters.head) // linter:disable:UndesirableTypeInference

    def toIterable(didx: Int) =
      new Iterable[Any] {
        def iterator = unbox(jc.getIterator(didx).asScala)
      }

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
