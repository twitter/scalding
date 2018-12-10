package com.twitter.scalding.typed.functions

import com.twitter.algebird.{ Aggregator, Ring, Semigroup, Fold }
import java.util.Random
import java.io.Serializable

case class Constant[T](result: T) extends Function1[Any, T] {
  def apply(a: Any) = result
}

case class ConstantKey[K, V](key: K) extends Function1[V, (K, V)] {
  def apply(v: V) = (key, v)
}

case class DebugFn[A]() extends Function1[A, A] {
  def apply(a: A) = {
    println(a)
    a
  }
}

case class WithConstant[A, B](constant: B) extends Function1[A, (A, B)] {
  def apply(a: A) = (a, constant)
}

case class MakeKey[K, V](fn: V => K) extends Function1[V, (K, V)] {
  def apply(v: V) = (fn(v), v)
}

case class MapOptionToFlatMap[A, B](fn: A => Option[B]) extends Function1[A, List[B]] {
  def apply(a: A) = fn(a) match {
    case None => Nil
    case Some(a) => a :: Nil
  }
}

case class PartialFunctionToFilter[A, B](fn: PartialFunction[A, B]) extends Function1[A, Boolean] {
  def apply(a: A) = fn.isDefinedAt(a)
}

case class MapValueStream[A, B](fn: Iterator[A] => Iterator[B]) extends Function2[Any, Iterator[A], Iterator[B]] {
  def apply(k: Any, vs: Iterator[A]) = fn(vs)
}

case class Drop[A](count: Int) extends Function1[Iterator[A], Iterator[A]] {
  def apply(as: Iterator[A]) = as.drop(count)
}
case class DropWhile[A](fn: A => Boolean) extends Function1[Iterator[A], Iterator[A]] {
  def apply(as: Iterator[A]) = as.dropWhile(fn)
}

case class Take[A](count: Int) extends Function1[Iterator[A], Iterator[A]] {
  def apply(as: Iterator[A]) = as.take(count)
}

case class TakeWhile[A](fn: A => Boolean) extends Function1[Iterator[A], Iterator[A]] {
  def apply(as: Iterator[A]) = as.takeWhile(fn)
}

case class Identity[A, B](eqTypes: EqTypes[A, B]) extends Function1[A, B] {
  def apply(a: A) = eqTypes(a)
}

object Identity extends Serializable {
  def apply[A](): Identity[A, A] = Identity[A, A](EqTypes.reflexive[A])
}

case class Widen[A, B](subTypes: SubTypes[A, B]) extends Function1[A, B] {
  def apply(a: A) = subTypes(a)
}

case class GetKey[K]() extends Function1[(K, Any), K] {
  def apply(kv: (K, Any)) = kv._1
}

case class GetValue[V]() extends Function1[(Any, V), V] {
  def apply(kv: (Any, V)) = kv._2
}

case class Swap[A, B]() extends Function1[(A, B), (B, A)] {
  def apply(ab: (A, B)) = (ab._2, ab._1)
}

case class SumAll[T](sg: Semigroup[T]) extends Function1[TraversableOnce[T], Iterator[T]] {
  def apply(ts: TraversableOnce[T]) = sg.sumOption(ts).iterator
}

case class Fill[A](size: Int) extends Function1[A, Iterator[A]] {
  def apply(a: A) = Iterator.fill(size)(a)
}

case class AggPrepare[A, B, C](agg: Aggregator[A, B, C]) extends Function1[A, B] {
  def apply(a: A) = agg.prepare(a)
}

case class AggPresent[A, B, C](agg: Aggregator[A, B, C]) extends Function1[B, C] {
  def apply(a: B) = agg.present(a)
}

case class FoldLeftIterator[A, B](init: B, fold: (B, A) => B) extends Function1[Iterator[A], Iterator[B]] {
  def apply(as: Iterator[A]) = Iterator.single(as.foldLeft(init)(fold))
}

case class ScanLeftIterator[A, B](init: B, fold: (B, A) => B) extends Function1[Iterator[A], Iterator[B]] {
  def apply(as: Iterator[A]) = as.scanLeft(init)(fold)
}

case class FoldIterator[A, B](fold: Fold[A, B]) extends Function1[Iterator[A], Iterator[B]] {
  def apply(as: Iterator[A]) = Iterator.single(fold.overTraversable(as))
}

case class FoldWithKeyIterator[K, A, B](foldfn: K => Fold[A, B]) extends Function2[K, Iterator[A], Iterator[B]] {
  def apply(k: K, as: Iterator[A]) = Iterator.single(foldfn(k).overTraversable(as))
}

case class AsRight[A, B]() extends Function1[B, Either[A, B]] {
  def apply(b: B) = Right(b)
}

case class AsLeft[A, B]() extends Function1[A, Either[A, B]] {
  def apply(b: A) = Left(b)
}

case class TuplizeFunction[A, B, C](fn: (A, B) => C) extends Function1[(A, B), C] {
  def apply(ab: (A, B)) = fn(ab._1, ab._2)
}

case class DropValue1[A, B, C]() extends Function1[(A, (B, C)), (A, C)] {
  def apply(abc: (A, (B, C))) = (abc._1, abc._2._2)
}

case class RandomNextInt(seed: Long, modulus: Int) extends Function1[Any, Int] {
  private[this] lazy val rng = new Random(seed)
  def apply(a: Any) = rng.nextInt(modulus)
}

case class RandomFilter(seed: Long, fraction: Double) extends Function1[Any, Boolean] {
  private[this] lazy val rng = new Random(seed)
  def apply(a: Any) = rng.nextDouble < fraction
}

case class Count[T](fn: T => Boolean) extends Function1[T, Long] {
  def apply(t: T) = if (fn(t)) 1L else 0L
}

case class SizeOfSet[T]() extends Function1[Set[T], Long] {
  def apply(s: Set[T]) = s.size.toLong
}

case class HeadSemigroup[T]() extends Semigroup[T] {
  def plus(a: T, b: T) = a
  // Don't enumerate every item, just take the first
  override def sumOption(to: TraversableOnce[T]): Option[T] =
    if (to.isEmpty) None
    else Some(to.toIterator.next)
}

case class SemigroupFromFn[T](fn: (T, T) => T) extends Semigroup[T] {
  def plus(a: T, b: T) = fn(a, b)
}

case class SemigroupFromProduct[T](ring: Ring[T]) extends Semigroup[T] {
  def plus(a: T, b: T) = ring.times(a, b)
}

/**
 * This is a semigroup that throws IllegalArgumentException if
 * there is more than one item. This is used to trigger optimizations
 * where the user knows there is at most one value per key.
 */
case class RequireSingleSemigroup[T]() extends Semigroup[T] {
  def plus(a: T, b: T) = throw new IllegalArgumentException(s"expected only one item, calling plus($a, $b)")
}

case class ConsList[T]() extends Function1[(T, List[T]), List[T]] {
  def apply(results: (T, List[T])) = results._1 :: results._2
}

case class ReverseList[T]() extends Function1[List[T], List[T]] {
  def apply(results: List[T]) = results.reverse
}

case class ToList[A]() extends Function1[Iterator[A], Iterator[List[A]]] {
  def apply(as: Iterator[A]) =
    // This should never really happen, but we are being defensive
    if (as.isEmpty) Iterator.empty
    else Iterator.single(as.toList)
}

case class ToSet[A]() extends Function1[A, Set[A]] {
  // this allows us to access Set1 without boxing into varargs
  private[this] val empty = Set.empty[A]
  def apply(a: A) = empty + a
}

case class MaxOrd[A, B >: A](ord: Ordering[B]) extends Function2[A, A, A] {
  def apply(a1: A, a2: A) =
    if (ord.lt(a1, a2)) a2 else a1
}

case class MaxOrdBy[A, B](fn: A => B, ord: Ordering[B]) extends Function2[A, A, A] {
  def apply(a1: A, a2: A) =
    if (ord.lt(fn(a1), fn(a2))) a2 else a1
}

case class MinOrd[A, B >: A](ord: Ordering[B]) extends Function2[A, A, A] {
  def apply(a1: A, a2: A) =
    if (ord.lt(a1, a2)) a1 else a2
}

case class MinOrdBy[A, B](fn: A => B, ord: Ordering[B]) extends Function2[A, A, A] {
  def apply(a1: A, a2: A) =
    if (ord.lt(fn(a1), fn(a2))) a1 else a2
}

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

case class EmptyGuard[K, A, B](fn: (K, Iterator[A]) => Iterator[B]) extends Function2[K, Iterator[A], Iterator[B]] {
  def apply(k: K, as: Iterator[A]) =
    if (as.nonEmpty) fn(k, as) else Iterator.empty
}

case class FilterGroup[A, B](fn: ((A, B)) => Boolean) extends Function2[A, Iterator[B], Iterator[B]] {
  def apply(a: A, bs: Iterator[B]) = bs.filter(fn(a, _))
}

case class MapGroupMapValues[A, B, C](fn: B => C) extends Function2[A, Iterator[B], Iterator[C]] {
  def apply(a: A, bs: Iterator[B]) = bs.map(fn)
}

case class MapGroupFlatMapValues[A, B, C](fn: B => TraversableOnce[C]) extends Function2[A, Iterator[B], Iterator[C]] {
  def apply(a: A, bs: Iterator[B]) = bs.flatMap(fn)
}

object FlatMapFunctions extends Serializable {
  case class FromIdentity[A]() extends Function1[A, Iterator[A]] {
    def apply(a: A) = Iterator.single(a)
  }
  case class FromFilter[A](fn: A => Boolean) extends Function1[A, Iterator[A]] {
    def apply(a: A) = if (fn(a)) Iterator.single(a) else Iterator.empty
  }
  case class FromMap[A, B](fn: A => B) extends Function1[A, Iterator[B]] {
    def apply(a: A) = Iterator.single(fn(a))
  }
  case class FromFilterCompose[A, B](fn: A => Boolean, next: A => TraversableOnce[B]) extends Function1[A, TraversableOnce[B]] {
    def apply(a: A) = if (fn(a)) next(a) else Iterator.empty
  }
  case class FromMapCompose[A, B, C](fn: A => B, next: B => TraversableOnce[C]) extends Function1[A, TraversableOnce[C]] {
    def apply(a: A) = next(fn(a))
  }
  case class FromFlatMapCompose[A, B, C](fn: A => TraversableOnce[B], next: B => TraversableOnce[C]) extends Function1[A, TraversableOnce[C]] {
    def apply(a: A) = fn(a).flatMap(next)
  }
}

object ComposedFunctions extends Serializable {

  case class ComposedMapFn[A, B, C](fn0: A => B, fn1: B => C) extends Function1[A, C] {
    def apply(a: A) = fn1(fn0(a))
  }
  case class ComposedFilterFn[-A](fn0: A => Boolean, fn1: A => Boolean) extends Function1[A, Boolean] {
    def apply(a: A) = fn0(a) && fn1(a)
  }
  /**
   * This is only called at the end of a task, so might as well make it stack safe since a little
   * extra runtime cost won't matter
   */
  case class ComposedOnComplete(fn0: () => Unit, fn1: () => Unit) extends Function0[Unit] {
    def apply(): Unit = {
      @annotation.tailrec
      def loop(fn: () => Unit, stack: List[() => Unit]): Unit =
        fn match {
          case ComposedOnComplete(left, right) => loop(left, right :: stack)
          case notComposed =>
            notComposed()
            stack match {
              case h :: tail => loop(h, tail)
              case Nil => ()
            }
        }

      loop(fn0, List(fn1))
    }
  }

  case class ComposedMapGroup[A, B, C, D](
    f: (A, Iterator[B]) => Iterator[C],
    g: (A, Iterator[C]) => Iterator[D]) extends Function2[A, Iterator[B], Iterator[D]] {

    def apply(a: A, bs: Iterator[B]) = {
      val cs = f(a, bs)
      if (cs.nonEmpty) g(a, cs)
      else Iterator.empty
    }
  }
}
