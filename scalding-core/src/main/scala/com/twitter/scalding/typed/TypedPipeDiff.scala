package com.twitter.scalding.typed

import scala.reflect.ClassTag

/**
 * Some methods for comparing two typed pipes and finding out the difference between them.
 *
 * Has support for the normal case where the typed pipes are pipes of objects usable as keys
 * in scalding (have an ordering, proper equals and hashCode), as well as some special cases
 * for dealing with Arrays and thrift objects.
 *
 * See diffByHashCode for comparing typed pipes of objects that have no ordering but a stable hash code
 * (such as Scrooge thrift).
 *
 * See diffByGroup for comparing typed pipes of objects that have no ordering *and* an unstable hash code.
 */
object TypedPipeDiff {

  /**
   * Returns a mapping from T to a count of the occurrences of T in the left and right pipes,
   * only for cases where the counts are not equal.
   *
   * Requires that T have an ordering and a hashCode and equals that is stable across JVMs (not reference based).
   * See diffArrayPipes for diffing pipes of arrays, since arrays do not meet these requirements by default.
   */
  def diff[T: Ordering](left: TypedPipe[T], right: TypedPipe[T], reducers: Option[Int] = None): UnsortedGrouped[T, (Long, Long)] = {
    val lefts = left.map { x => (x, (1L, 0L)) }
    val rights = right.map { x => (x, (0L, 1L)) }
    val counts = (lefts ++ rights).sumByKey
    val diff = counts.filter { case (key, (lCount, rCount)) => lCount != rCount }
    reducers.map(diff.withReducers).getOrElse(diff)
  }

  /**
   * Same as diffByHashCode, but takes care to wrap the Array[T] in a wrapper,
   * which has the correct hashCode and equals needed. This does not involve
   * copying the arrays, just wrapping them, and is specialized for primitive arrays.
   */
  def diffArrayPipes[T: ClassTag](left: TypedPipe[Array[T]],
    right: TypedPipe[Array[T]],
    reducers: Option[Int] = None): TypedPipe[(Array[T], (Long, Long))] = {

    // cache this instead of reflecting on every single array
    val wrapFn = HashEqualsArrayWrapper.wrapByClassTagFn[T]

    diffByHashCode(left.map(wrapFn), right.map(wrapFn), reducers)
      .map { case (k, counts) => (k.wrapped, counts) }
  }

  /**
   * NOTE: Prefer diff over this method if you can find or construct an Ordering[T].
   *
   * Returns a mapping from T to a count of the occurrences of T in the left and right pipes,
   * only for cases where the counts are not equal.
   *
   * This implementation does not require an ordering on T, but does require a function (groupByFn)
   * that extracts a value of type K (which has an ordering) from a record of type T.
   *
   * The groupByFn should be something that partitions records as evenly as possible,
   * because all unique records that result in the same groupByFn value will be materialized into an in memory map.
   *
   * groupByFn must be a pure function, such that:
   * x == y implies that groupByFn(x) == groupByFn(y)
   *
   * T must have a hash code suitable for use in a hash map on a single JVM (doesn't have to be stable cross JVM)
   * K must have a hash code this *is* stable across JVMs.
   * K must have an ordering.
   *
   * Example groupByFns would be x => x.hashCode, assuming x's hashCode is stable across jvms,
   * or maybe x => x.timestamp, if x's hashCode is not stable, assuming there's shouldn't be too
   * many records with the same timestamp.
   */
  def diffByGroup[T, K: Ordering](
    left: TypedPipe[T],
    right: TypedPipe[T],
    reducers: Option[Int] = None)(groupByFn: T => K): TypedPipe[(T, (Long, Long))] = {

    val lefts = left.map { t => (groupByFn(t), Map(t -> (1L, 0L))) }
    val rights = right.map { t => (groupByFn(t), Map(t -> (0L, 1L))) }

    val diff = (lefts ++ rights)
      .sumByKey
      .flattenValues
      .filter { case (k, (t, (lCount, rCount))) => lCount != rCount }

    reducers.map(diff.withReducers).getOrElse(diff).values
  }

  /**
   * NOTE: Prefer diff over this method if you can find or construct an Ordering[T].
   *
   * Same as diffByGroup but uses T.hashCode as the groupByFn
   *
   * This method does an exact diff, it does not use the hashCode as a proxy for equality.
   */
  def diffByHashCode[T](
    left: TypedPipe[T],
    right: TypedPipe[T],
    reducers: Option[Int] = None): TypedPipe[(T, (Long, Long))] = diffByGroup(left, right, reducers)(_.hashCode)

  object Enrichments {

    implicit class Diff[T](val left: TypedPipe[T]) extends AnyVal {

      def diff(right: TypedPipe[T], reducers: Option[Int] = None)(implicit ev: Ordering[T]): UnsortedGrouped[T, (Long, Long)] =
        TypedPipeDiff.diff(left, right, reducers)

      def diffByGroup[K: Ordering](right: TypedPipe[T], reducers: Option[Int] = None)(groupByFn: T => K): TypedPipe[(T, (Long, Long))] =
        TypedPipeDiff.diffByGroup(left, right, reducers)(groupByFn)

      def diffByHashCode(right: TypedPipe[T], reducers: Option[Int] = None): TypedPipe[(T, (Long, Long))] = TypedPipeDiff.diffByHashCode(left, right, reducers)
    }

    implicit class DiffArray[T](val left: TypedPipe[Array[T]]) extends AnyVal {

      def diffArrayPipes(right: TypedPipe[Array[T]], reducers: Option[Int] = None)(implicit ev: ClassTag[T]): TypedPipe[(Array[T], (Long, Long))] =
        TypedPipeDiff.diffArrayPipes(left, right, reducers)
    }

  }
}
