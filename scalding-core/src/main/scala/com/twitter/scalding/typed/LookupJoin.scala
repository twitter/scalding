/*
 Copyright 2013 Twitter, Inc.

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

import java.io.Serializable

import com.twitter.algebird.Semigroup

/*
 Copyright 2013 Twitter, Inc.

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

/**
 * lookupJoin simulates the behavior of a realtime system attempting
 * to leftJoin (K, V) pairs against some other value type (JoinedV)
 * by performing realtime lookups on a key-value Store.
 *
 * An example would join (K, V) pairs of (URL, Username) against a
 * service of (URL, ImpressionCount). The result of this join would
 * be a pipe of (ShortenedURL, (Username,
 * Option[ImpressionCount])).
 *
 * To simulate this behavior, lookupJoin accepts pipes of key-value
 * pairs with an explicit time value T attached. T must have some
 * sensible ordering. The semantics are, if one were to hit the
 * right pipe's simulated realtime service at any time between
 * T(tuple) T(tuple + 1), one would receive Some((K,
 * JoinedV)(tuple)).
 *
 * The entries in the left pipe's tuples have the following
 * meaning:
 *
 * T: The  time at which the (K, W) lookup occurred.
 * K: the join key.
 * W: the current value for the join key.
 *
 * The right pipe's entries have the following meaning:
 *
 * T: The time at which the "service" was fed an update
 * K: the join K.
 * V: value of the key at time T
 *
 * Before the time T in the right pipe's very first entry, the
 * simulated "service" will return None. After this time T, the
 * right side will return None only if the key is absent,
 * else, the service will return Some(joinedV).
 */

object LookupJoin extends Serializable {

  /**
   * This is the "infinite history" join and always joins regardless of how
   * much time is between the left and the right
   */

  def apply[T: Ordering, K: Ordering, V, JoinedV](
    left: TypedPipe[(T, (K, V))],
    right: TypedPipe[(T, (K, JoinedV))],
    reducers: Option[Int] = None): TypedPipe[(T, (K, (V, Option[JoinedV])))] =

    withWindow(left, right, reducers)((_, _) => true)

  /**
   * In this case, the right pipe is fed through a scanLeft doing a Semigroup.plus
   * before joined to the left
   */
  def rightSumming[T: Ordering, K: Ordering, V, JoinedV: Semigroup](left: TypedPipe[(T, (K, V))],
    right: TypedPipe[(T, (K, JoinedV))],
    reducers: Option[Int] = None): TypedPipe[(T, (K, (V, Option[JoinedV])))] =
    withWindowRightSumming(left, right, reducers)((_, _) => true)

  /**
   * This ensures that gate(Tleft, Tright) == true, else the None is emitted
   * as the joined value.
   * Useful for bounding the time of the join to a recent window
   */
  def withWindow[T: Ordering, K: Ordering, V, JoinedV](left: TypedPipe[(T, (K, V))],
    right: TypedPipe[(T, (K, JoinedV))],
    reducers: Option[Int] = None)(gate: (T, T) => Boolean): TypedPipe[(T, (K, (V, Option[JoinedV])))] = {

    implicit val keepNew: Semigroup[JoinedV] = Semigroup.from { (older, newer) => newer }
    withWindowRightSumming(left, right, reducers)(gate)
  }

  /**
   * This ensures that gate(Tleft, Tright) == true, else the None is emitted
   * as the joined value, and sums are only done as long as they they come
   * within the gate interval as well
   */
  def withWindowRightSumming[T: Ordering, K: Ordering, V, JoinedV: Semigroup](left: TypedPipe[(T, (K, V))],
    right: TypedPipe[(T, (K, JoinedV))],
    reducers: Option[Int] = None)(gate: (T, T) => Boolean): TypedPipe[(T, (K, (V, Option[JoinedV])))] = {
    /**
     * Implicit ordering on an either that doesn't care about the
     * actual container values, puts the lookups before the service writes
     * Since we assume it takes non-zero time to do a lookup.
     */
    implicit def eitherOrd[T, U]: Ordering[Either[T, U]] =
      new Ordering[Either[T, U]] {
        def compare(l: Either[T, U], r: Either[T, U]) =
          (l, r) match {
            case (Left(_), Right(_)) => -1
            case (Right(_), Left(_)) => 1
            case (Left(_), Left(_)) => 0
            case (Right(_), Right(_)) => 0
          }
      }

    val joined: TypedPipe[(K, (Option[(T, JoinedV)], Option[(T, V, Option[JoinedV])]))] =
      left.map { case (t, (k, v)) => (k, (t, Left(v): Either[V, JoinedV])) }
        .++(right.map {
          case (t, (k, joinedV)) =>
            (k, (t, Right(joinedV): Either[V, JoinedV]))
        })
        .group
        .withReducers(reducers.getOrElse(-1)) // -1 means default in scalding
        .sorted
        /**
         * Grouping by K leaves values of (T, Either[V, JoinedV]). Sort
         * by time and scanLeft. The iterator will now represent pairs of
         * T and either new values to join against or updates to the
         * simulated "realtime store" described above.
         */
        .scanLeft(
          /**
           * In the simulated realtime store described above, this
           * None is the value in the store at the current
           * time. Because we sort by time and scan forward, this
           * value will be updated with a new value every time a
           * Right(delta) shows up in the iterator.
           *
           * The second entry in the pair will be None when the
           * JoinedV is updated and Some(newValue) when a (K, V)
           * shows up and a new join occurs.
           */
          (Option.empty[(T, JoinedV)], Option.empty[(T, V, Option[JoinedV])])) {
            case ((None, result), (time, Left(v))) => {
              // The was no value previously
              (None, Some((time, v, None)))
            }

            case ((prev @ Some((oldt, jv)), result), (time, Left(v))) => {
              // Left(v) means that we have a new value from the left
              // pipe that we need to join against the current
              // "lastJoined" value sitting in scanLeft's state. This
              // is equivalent to a lookup on the data in the right
              // pipe at time "thisTime".
              val filteredJoined = if (gate(time, oldt)) Some(jv) else None
              (prev, Some((time, v, filteredJoined)))
            }

            case ((None, result), (time, Right(joined))) => {
              // There was no value before, so we just update to joined
              (Some((time, joined)), None)
            }

            case ((Some((oldt, oldJ)), result), (time, Right(joined))) => {
              // Right(joinedV) means that we've received a new value
              // to use in the simulated realtime service
              // described in the comments above
              // did it fall out of cache?
              val nextJoined = if (gate(time, oldt)) Semigroup.plus(oldJ, joined) else joined
              (Some((time, nextJoined)), None)
            }
          }.toTypedPipe

    // Now, get rid of residual state from the scanLeft above:
    joined.flatMap {
      case (k, (_, optV)) =>
        // filter out every event that produced a Right(delta) above,
        // leaving only the leftJoin events that occurred above:
        optV.map {
          case (t, v, optJoined) => (t, (k, (v, optJoined)))
        }
    }
  }
}
