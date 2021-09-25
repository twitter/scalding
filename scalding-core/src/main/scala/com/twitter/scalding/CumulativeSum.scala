package com.twitter.scalding.typed

import com.twitter.algebird._

/**
 * Extension for TypedPipe to add a cumulativeSum method.
 * Given a TypedPipe with T = (GroupField, (SortField, SummableField))
 * cumulaitiveSum will return a SortedGrouped with the SummableField accumulated
 * according to the sort field.
 * eg:
 *  ('San Francisco', (100, 100)),
 *  ('San Francisco', (101, 50)),
 *  ('San Francisco', (200, 200)),
 *  ('Vancouver', (100, 50)),
 *  ('Vancouver', (101, 300)),
 *  ('Vancouver', (200, 100))
 * becomes
 *  ('San Francisco', (100, 100)),
 *  ('San Francisco', (101, 150)),
 *  ('San Francisco', (200, 300)),
 *  ('Vancouver', (100, 50)),
 *  ('Vancouver', (101, 350)),
 *  ('Vancouver', (200, 450))
 *
 * If you provide cumulativeSum a partition function you get the same result
 * but you allow for more than one reducer per group. This is useful for
 * when you have a single group that has a very large number of entries.
 * For example in the previous example if you gave a partition function of the
 * form { _ / 100 } then you would never have any one reducer deal with more
 * than 2 entries.
 */
object CumulativeSum {
  implicit def toCumulativeSum[K, U, V](pipe: TypedPipe[(K, (U, V))]): CumulativeSumExtension[K, U, V] =
    new CumulativeSumExtension(pipe)

  class CumulativeSumExtension[K, U, V](
    val pipe: TypedPipe[(K, (U, V))]) {
    /** Takes a sortable field and a monoid and returns the cumulative sum of that monoid **/
    def cumulativeSum(
      implicit sg: Semigroup[V],
      ordU: Ordering[U],
      ordK: Ordering[K]): SortedGrouped[K, (U, V)] = {
      pipe.group
        .sortBy { case (u, _) => u }
        .scanLeft(Nil: List[(U, V)]) {
          case (acc, (u, v)) =>
            acc match {
              case List((previousU, previousSum)) => List((u, sg.plus(previousSum, v)))
              case _ => List((u, v))
            }
        }
        .flattenValues
    }
    /**
     * An optimization of cumulativeSum for cases when a particular key has many
     * entries. Requires a sortable partitioning of U.
     * Accomplishes the optimization by not requiring all the entries for a
     * single key to go through a single scan. Instead requires the sums of the
     * partitions for a single key to go through a single scan.
     */
    def cumulativeSum[S](partition: U => S)(
      implicit ordS: Ordering[S],
      sg: Semigroup[V],
      ordU: Ordering[U],
      ordK: Ordering[K]): TypedPipe[(K, (U, V))] = {

      val sumPerS = pipe
        .map { case (k, (u, v)) => (k, partition(u)) -> v }
        .sumByKey
        .map { case ((k, s), v) => (k, (s, v)) }
        .group
        .sortBy { case (s, v) => s }
        .scanLeft(None: Option[(Option[V], V, S)]) {
          case (acc, (s, v)) =>
            acc match {
              case Some((previousPreviousSum, previousSum, previousS)) => {
                Some((Some(previousSum), sg.plus(v, previousSum), s))
              }
              case _ => Some((None, v, s))
            }
        }
        .flatMap{
          case (k, maybeAcc) =>
            for (
              acc <- maybeAcc;
              previousSum <- acc._1
            ) yield { (k, acc._3) -> (None, previousSum) }
        }

      val summands = pipe
        .map {
          case (k, (u, v)) =>
            (k, partition(u)) -> (Some(u), v)
        } ++ sumPerS

      summands
        .group
        .sortBy { case (u, _) => u }
        .scanLeft(None: Option[(Option[U], V)]) {
          case (acc, (maybeU, v)) =>
            acc match {
              case Some((_, previousSum)) => Some((maybeU, sg.plus(v, previousSum)))
              case _ => Some((maybeU, v))
            }
        }
        .flatMap {
          case ((k, s), acc) =>
            for (uv <- acc; u <- uv._1) yield {
              (k, (u, uv._2))
            }
        }
    }
  }
}

