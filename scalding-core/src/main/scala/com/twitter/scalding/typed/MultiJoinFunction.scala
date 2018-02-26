package com.twitter.scalding.typed

import com.twitter.scalding.serialization.Externalizer
import java.io.Serializable

/**
 * This is a weakly typed multi-way join function. By construction,
 * it should be kept in sync with the types in a Seq[TypedPipe[(K, Any)]]
 *
 * a more sophisticated typing could use an HList of TypedPipe
 * and another more advanced coding here to prove the types line up.
 * However, this is somewhat easy to test and only exposed to
 * those writing backends, so we are currently satisfied with the
 * weak typing in this case
 *
 * We use Externalizer internally to independently serialize each function
 * in the composition. This, in principle, should allow Externalizer
 * to work better since different functions may be serializable with
 * Kryo or Java, but currently Externalizer has to use java or kryo
 * for the entire object.
 */
sealed abstract class MultiJoinFunction[A, +B] extends Serializable {
  def inputSize: Int
  def apply(key: A, leftMost: Iterator[Any], rightStreams: Seq[Iterable[Any]]): Iterator[B]
}

object MultiJoinFunction extends Serializable {
  final case class Casting[A, B]() extends MultiJoinFunction[A, B] {
    def inputSize = 1
    def apply(k: A, iter: Iterator[Any], empties: Seq[Iterable[Any]]) = {
      require(empties.isEmpty, "this join function should never be called with non-empty right-most")
      iter.asInstanceOf[Iterator[B]]
    }
  }

  final case class PairCachedRight[K, A, B, C](
    left: MultiJoinFunction[K, A],
    right: MultiJoinFunction[K, B],
    @transient fn: (K, Iterator[A], Iterable[B]) => Iterator[C]) extends MultiJoinFunction[K, C] {

    private[this] val fnEx = Externalizer(fn)

    val inputSize = left.inputSize + right.inputSize
    private[this] val leftSeqCount = left.inputSize - 1

    def apply(key: K, leftMost: Iterator[Any], rightStreams: Seq[Iterable[Any]]): Iterator[C] = {
      /*
       * This require is just an extra check (which should never possibly fail unless we have a programming bug)
       * that the number of streams we are joining matches the total joining operation we have.
       *
       * Since we have one stream in leftMost, the others should be in rightStreams.
       *
       * This check is cheap compared with the whole join, so we put this here to aid in checking
       * correctness due to the weak types that MultiJoinFunction has (non-static size of Seq and
       * the use of Any)
       */
      require(rightStreams.size == inputSize - 1, s"expected ${inputSize} inputSize, found ${rightStreams.size + 1}")
      val (leftSeq, rightSeq) = rightStreams.splitAt(leftSeqCount)
      val joinedLeft = left(key, leftMost, leftSeq)

      // we should materialize the final right one time:
      val joinedRight = right(key, rightSeq.head.iterator, rightSeq.tail).toList
      fnEx.get(key, joinedLeft, joinedRight)
    }
  }

  final case class Pair[K, A, B, C](
    left: MultiJoinFunction[K, A],
    right: MultiJoinFunction[K, B],
    @transient fn: (K, Iterator[A], Iterable[B]) => Iterator[C]) extends MultiJoinFunction[K, C] {

    private[this] val fnEx = Externalizer(fn)

    val inputSize = left.inputSize + right.inputSize
    private[this] val leftSeqCount = left.inputSize - 1

    def apply(key: K, leftMost: Iterator[Any], rightStreams: Seq[Iterable[Any]]): Iterator[C] = {
      /*
       * This require is just an extra check (which should never possibly fail unless we have a programming bug)
       * that the number of streams we are joining matches the total joining operation we have.
       *
       * Since we have one stream in leftMost, the others should be in rightStreams.
       *
       * This check is cheap compared with the whole join, so we put this here to aid in checking
       * correctness due to the weak types that MultiJoinFunction has (non-static size of Seq and
       * the use of Any)
       */
      require(rightStreams.size == inputSize - 1, s"expected ${inputSize} inputSize, found ${rightStreams.size + 1}")
      val (leftSeq, rightSeq) = rightStreams.splitAt(leftSeqCount)
      val joinedLeft = left(key, leftMost, leftSeq)

      // Only do this once, for all calls to iterator below
      val smallerHead = rightSeq.head // linter:disable:UndesirableTypeInference
      val smallerTail = rightSeq.tail

      // TODO: it might make sense to cache this in memory as an IndexedSeq and not
      // recompute it on every value for the left if the smallerJf is non-trivial
      // we could see how long it is, and possible switch to a cached version the
      // second time through if it is small enough
      val joinedRight = new Iterable[B] {
        def iterator = right(key, smallerHead.iterator, smallerTail)
      }

      fnEx.get(key, joinedLeft, joinedRight)
    }
  }

  /**
   * This is used to implement mapGroup on already joined streams
   */
  final case class MapGroup[K, A, B](
    input: MultiJoinFunction[K, A],
    @transient mapGroupFn: (K, Iterator[A]) => Iterator[B]) extends MultiJoinFunction[K, B] {

    private[this] val fnEx = Externalizer(mapGroupFn)

    def inputSize = input.inputSize

    def apply(key: K, leftMost: Iterator[Any], rightStreams: Seq[Iterable[Any]]): Iterator[B] = {
      val joined = input(key, leftMost, rightStreams)
      fnEx.get(key, joined)
    }
  }

  /**
   * This is used to join IteratorMappedReduce with others.
   * We could compose Casting[A] with MapGroup[K, A, B] but since it is common enough we give
   * it its own case.
   */
  final case class MapCast[K, A, B](@transient mapGroupFn: (K, Iterator[A]) => Iterator[B]) extends MultiJoinFunction[K, B] {

    private[this] val fnEx = Externalizer(mapGroupFn)

    def inputSize = 1

    def apply(key: K, leftMost: Iterator[Any], rightStreams: Seq[Iterable[Any]]): Iterator[B] = {
      require(rightStreams.isEmpty, "this join function should never be called with non-empty right-most")
      fnEx.get(key, leftMost.asInstanceOf[Iterator[A]])
    }
  }
}

