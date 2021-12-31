package com.twitter.scalding.beam_backend

import com.twitter.scalding.serialization.Externalizer
import com.twitter.scalding.typed.Joiner
import com.twitter.scalding.typed
import java.io.Serializable

object BeamJoiner {
  private def beamMapGroupJoin[K, R, R1](
      fn: (K, Iterator[R]) => Iterator[R1]
  ): (K, Iterable[R]) => Iterable[R1] = { (k: K, iter: Iterable[R]) =>
    new Iterable[R1] {
      def iterator: Iterator[R1] = fn(k, iter.iterator)
    }
  }

  private def beamHashJoin[K, V1, V2, R](
      hj: (K, V1, Iterable[V2]) => Iterator[R]
  ): HashJoinFunction[K, V1, V2, R] =
    hj match {
      case _: Joiner.HashInner[K, v, u]                   => HashInner()
      case _: Joiner.HashLeft[K, v, u]                    => HashLeft()
      case f: Joiner.FilteredHashJoin[K, v1, v2, r]       => FilteredHashJoin(beamHashJoin(f.jf), f.fn)
      case f: Joiner.MappedHashJoin[K, v1, v2, r, r1]     => MappedHashJoin(beamHashJoin(f.jf), f.fn)
      case f: Joiner.FlatMappedHashJoin[K, v1, v2, r, r1] => FlatMappedHashJoin(beamHashJoin(f.jf), f.fn)
      case f                                              => ArbitraryHashJoin(f)
    }

  private def beamJoin[K, A, B, C](
      fn: (K, Iterator[A], Iterable[B]) => Iterator[C]
  ): JoinFunction[K, A, B, C] =
    fn match {
      case _: Joiner.LeftJoin[K, v1, v2]                 => LeftJoin()
      case _: Joiner.RightJoin[K, v1, v2]                => RightJoin()
      case _: Joiner.OuterJoin[K, v1, v2]                => OuterJoin()
      case _: Joiner.InnerJoin[K, v1, v2]                => InnerJoin()
      case join: Joiner.FilteredJoin[K, v1, v2, r]       => FilteredJoin(beamJoin(join.jf), join.fn)
      case join: Joiner.MappedJoin[K, v1, v2, r, r1]     => MappedJoin(beamJoin(join.jf), join.fn)
      case join: Joiner.FlatMappedJoin[K, v1, v2, r, r1] => FlatMappedJoin(beamJoin(join.jf), join.fn)
      case join: Joiner.MappedGroupJoin[K, v1, v2, r, r1] =>
        MappedGroupJoin(beamJoin(join.jf), beamMapGroupJoin(join.fn))
      case join: Joiner.JoinFromHashJoin[K, v1, v2, r] => JoinFromHashJoin(beamHashJoin(join.hj))
      case join                                        => ArbitraryJoin(join)
    }

  def beamMultiJoin[A, B](m: typed.MultiJoinFunction[A, B]): MultiJoinFunction[A, B] =
    m match {
      case _: typed.MultiJoinFunction.Casting[A, B] => MultiJoinFunction.Casting[A, B]()
      case join: typed.MultiJoinFunction.PairCachedRight[A, x, y, B] =>
        MultiJoinFunction
          .PairCachedRight[A, x, y, B](beamMultiJoin(join.left), beamMultiJoin(join.right), beamJoin(join.fn))
      case join: typed.MultiJoinFunction.Pair[A, x, y, B] =>
        MultiJoinFunction
          .Pair[A, x, y, B](beamMultiJoin(join.left), beamMultiJoin(join.right), beamJoin(join.fn))
      case join: typed.MultiJoinFunction.MapGroup[A, x, B] =>
        MultiJoinFunction.MapGroup[A, x, B](beamMultiJoin(join.input), beamMapGroupJoin(join.mapGroupFn))
      case join: typed.MultiJoinFunction.MapCast[A, x, B] =>
        MultiJoinFunction.MapCast[A, x, B](beamMapGroupJoin(join.mapGroupFn))
    }

  sealed abstract class MultiJoinFunction[A, +B] extends Serializable {
    def inputSize: Int
    def apply(key: A, streams: Seq[Iterable[Any]]): Iterable[B]
  }

  object MultiJoinFunction extends Serializable {
    final case class Casting[A, B]() extends MultiJoinFunction[A, B] {
      override def inputSize: Int = 1
      override def apply(key: A, streams: Seq[Iterable[Any]]): Iterable[B] = {
        require(streams.size == 1, "this join function should never be called with multiple streams")
        streams.head.asInstanceOf[Iterable[B]]
      }
    }

    final case class PairCachedRight[K, A, B, C](
        left: MultiJoinFunction[K, A],
        right: MultiJoinFunction[K, B],
        @transient fn: (K, Iterable[A], Iterable[B]) => Iterable[C]
    ) extends MultiJoinFunction[K, C] {

      private[this] val fnEx = Externalizer(fn)

      override val inputSize: Int = left.inputSize + right.inputSize

      def apply(key: K, streams: Seq[Iterable[Any]]): Iterable[C] = {
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
        require(streams.size == inputSize, s"expected $inputSize inputSize, found ${streams.size}")
        val (leftSeq, rightSeq) = streams.splitAt(left.inputSize)
        val joinedLeft = left(key, leftSeq)

        // we should materialize the final right one time:
        val joinedRight = right(key, rightSeq).toList
        fnEx.get(key, joinedLeft, joinedRight)
      }
    }

    final case class Pair[K, A, B, C](
        left: MultiJoinFunction[K, A],
        right: MultiJoinFunction[K, B],
        @transient fn: (K, Iterable[A], Iterable[B]) => Iterable[C]
    ) extends MultiJoinFunction[K, C] {

      private[this] val fnEx = Externalizer(fn)

      override val inputSize: Int = left.inputSize + right.inputSize

      def apply(key: K, streams: Seq[Iterable[Any]]): Iterable[C] = {
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
        require(streams.size == inputSize, s"expected $inputSize inputSize, found ${streams.size}")
        val (leftSeq, rightSeq) = streams.splitAt(left.inputSize)
        val joinedLeft = left(key, leftSeq)

        // TODO: it might make sense to cache this in memory as an IndexedSeq and not
        // recompute it on every value for the left if the smallerJf is non-trivial
        // we could see how long it is, and possible switch to a cached version the
        // second time through if it is small enough
        val joinedRight = right(key, rightSeq)

        fnEx.get(key, joinedLeft, joinedRight)
      }
    }

    /**
     * This is used to implement mapGroup on already joined streams
     */
    final case class MapGroup[K, A, B](
        input: MultiJoinFunction[K, A],
        @transient mapGroupFn: (K, Iterable[A]) => Iterable[B]
    ) extends MultiJoinFunction[K, B] {

      private[this] val fnEx = Externalizer(mapGroupFn)

      def inputSize = input.inputSize

      def apply(key: K, streams: Seq[Iterable[Any]]): Iterable[B] = {
        val joined = input(key, streams)
        fnEx.get(key, joined)
      }
    }

    /**
     * This is used to join IteratorMappedReduce with others. We could compose Casting[A] with MapGroup[K, A,
     * B] but since it is common enough we give it its own case.
     */
    final case class MapCast[K, A, B](@transient mapGroupFn: (K, Iterable[A]) => Iterable[B])
        extends MultiJoinFunction[K, B] {

      private[this] val fnEx = Externalizer(mapGroupFn)

      def inputSize = 1

      def apply(key: K, streams: Seq[Iterable[Any]]): Iterable[B] = {
        require(streams.size == 1, "this join function should never be called with multiple streams")
        fnEx.get(key, streams.head.asInstanceOf[Iterable[A]])
      }
    }
  }

  def asOuter[U](it: Iterable[U]): Iterable[Option[U]] =
    if (it.isEmpty) Iterable(None)
    else it.map(Some(_))

  /**
   * Optimizers want to match on the kinds of joins we are doing. This gives them that ability
   */
  sealed abstract class HashJoinFunction[-K, -V, -U, +R] extends Function3[K, V, Iterable[U], Iterable[R]]

  final case class HashInner[K, V, U]() extends HashJoinFunction[K, V, U, (V, U)] {
    def apply(k: K, v: V, u: Iterable[U]) = u.map((v, _))
  }
  final case class HashLeft[K, V, U]() extends HashJoinFunction[K, V, U, (V, Option[U])] {
    def apply(k: K, v: V, u: Iterable[U]) = asOuter(u).map((v, _))
  }
  final case class FilteredHashJoin[K, V1, V2, R](jf: HashJoinFunction[K, V1, V2, R], fn: ((K, R)) => Boolean)
      extends HashJoinFunction[K, V1, V2, R] {
    def apply(k: K, left: V1, right: Iterable[V2]) =
      jf.apply(k, left, right).filter(r => fn((k, r)))
  }
  final case class MappedHashJoin[K, V1, V2, R, R1](jf: HashJoinFunction[K, V1, V2, R], fn: R => R1)
      extends HashJoinFunction[K, V1, V2, R1] {
    def apply(k: K, left: V1, right: Iterable[V2]) =
      jf.apply(k, left, right).map(fn)
  }
  final case class FlatMappedHashJoin[K, V1, V2, R, R1](
      jf: HashJoinFunction[K, V1, V2, R],
      fn: R => TraversableOnce[R1]
  ) extends HashJoinFunction[K, V1, V2, R1] {
    def apply(k: K, left: V1, right: Iterable[V2]) =
      jf.apply(k, left, right).flatMap(fn)
  }
  final case class ArbitraryHashJoin[K, V1, V2, R](
      hj: (K, V1, Iterable[V2]) => Iterator[R]
  ) extends HashJoinFunction[K, V1, V2, R] {
    def apply(k: K, left: V1, right: Iterable[V2]): Iterable[R] =
      new Iterable[R] {
        def iterator: Iterator[R] = hj.apply(k, left, right)
      }
  }

  /**
   * As opposed to Scalding's JoinFunction, in Beam we make 'right' be the one iterated once and 'left' many
   * times It replaces all uses of Iterator with Iterable since Beam can always provide Iterables.
   */
  sealed abstract class JoinFunction[-K, -V1, -V2, +R]
      extends ((K, Iterable[V1], Iterable[V2]) => Iterable[R])

  final case class InnerJoin[K, V1, V2]() extends JoinFunction[K, V1, V2, (V1, V2)] {
    def apply(k: K, left: Iterable[V1], right: Iterable[V2]): Iterable[(V1, V2)] =
      right.flatMap(v2 => left.map((_, v2)))
  }
  final case class LeftJoin[K, V1, V2]() extends JoinFunction[K, V1, V2, (V1, Option[V2])] {
    def apply(k: K, left: Iterable[V1], right: Iterable[V2]): Iterable[(V1, Option[V2])] =
      asOuter(right).flatMap(v2 => left.map((_, v2)))
  }
  final case class RightJoin[K, V1, V2]() extends JoinFunction[K, V1, V2, (Option[V1], V2)] {
    def apply(k: K, left: Iterable[V1], right: Iterable[V2]): Iterable[(Option[V1], V2)] =
      right.flatMap(v2 => asOuter(left).map((_, v2)))
  }
  final case class OuterJoin[K, V1, V2]() extends JoinFunction[K, V1, V2, (Option[V1], Option[V2])] {
    def apply(k: K, left: Iterable[V1], right: Iterable[V2]): Iterable[(Option[V1], Option[V2])] =
      if (left.isEmpty && right.isEmpty) Iterable.empty
      else asOuter(right).flatMap(v2 => asOuter(left).map((_, v2)))
  }
  final case class FilteredJoin[K, V1, V2, R](jf: JoinFunction[K, V1, V2, R], fn: ((K, R)) => Boolean)
      extends JoinFunction[K, V1, V2, R] {
    def apply(k: K, left: Iterable[V1], right: Iterable[V2]): Iterable[R] =
      jf.apply(k, left, right).filter(r => fn((k, r)))
  }
  final case class MappedJoin[K, V1, V2, R, R1](jf: JoinFunction[K, V1, V2, R], fn: R => R1)
      extends JoinFunction[K, V1, V2, R1] {
    def apply(k: K, left: Iterable[V1], right: Iterable[V2]): Iterable[R1] =
      jf.apply(k, left, right).map(fn)
  }
  final case class FlatMappedJoin[K, V1, V2, R, R1](
      jf: JoinFunction[K, V1, V2, R],
      fn: R => TraversableOnce[R1]
  ) extends JoinFunction[K, V1, V2, R1] {
    def apply(k: K, left: Iterable[V1], right: Iterable[V2]): Iterable[R1] =
      jf.apply(k, left, right).flatMap(fn)
  }
  final case class MappedGroupJoin[K, V1, V2, R, R1](
      jf: JoinFunction[K, V1, V2, R],
      fn: (K, Iterable[R]) => Iterable[R1]
  ) extends JoinFunction[K, V1, V2, R1] {
    def apply(k: K, left: Iterable[V1], right: Iterable[V2]): Iterable[R1] = {
      val iterr = jf.apply(k, left, right)
      if (iterr.isEmpty) Iterable.empty // mapGroup operates on non-empty groups
      else fn(k, iterr)
    }
  }
  final case class JoinFromHashJoin[K, V1, V2, R](hj: (K, V1, Iterable[V2]) => Iterable[R])
      extends JoinFunction[K, V1, V2, R] {
    def apply(k: K, left: Iterable[V1], right: Iterable[V2]): Iterable[R] =
      left.flatMap(hj(k, _, right))
  }

  final case class ArbitraryJoin[K, V1, V2, R](fn: (K, Iterator[V1], Iterable[V2]) => Iterator[R])
      extends JoinFunction[K, V1, V2, R] {
    def apply(k: K, left: Iterable[V1], right: Iterable[V2]): Iterable[R] =
      new Iterable[R] {
        def iterator: Iterator[R] = fn(k, left.iterator, right)
      }
  }

}
