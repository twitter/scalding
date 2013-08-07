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
  * sensical ordering. The semantics are, if one were to hit the
  * right pipe's simulated realtime service at any time between
  * T(tuple) T(tuple + 1), one would receive Some((K,
  * JoinedV)(tuple)).
  *
  * The entries in the left pipe's tuples have the following
  * meaning:
  *
  * T: The the time at which the (K, W) lookup occurred.
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
  def apply[T:Ordering, K:Ordering, V, JoinedV](left: TypedPipe[(T, (K, V))], right: TypedPipe[(T, (K, JoinedV))]):
      TypedPipe[(T, (K, (V, Option[JoinedV])))] = {
    /**
      * Implicit ordering on an either that doesn't care about the
      * actual container values, puts the lookups before the service
      * writes Since we assume it takes non-zero time to do a lookup.
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

    val joined: TypedPipe[(K, (Option[JoinedV], Option[(T, V, Option[JoinedV])]))] =
      left.map { case (t, (k, v)) => (k, (t, Left(v): Either[V, JoinedV])) }
        .++(right.map { case (t, (k, joinedV)) => (k, (t, Right(joinedV): Either[V, JoinedV])) })
        .group
        .sortBy { _._2 }
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
          (None: Option[JoinedV], None: Option[(T, V, Option[JoinedV])])
        ) { case ((lastJoined, _), (thisTime, leftOrRight)) =>
            leftOrRight match {
              // Left(v) means that we have a new value from the left
              // pipe that we need to join against the current
              // "lastJoined" value sitting in scanLeft's state. This
              // is equivalent to a lookup on the data in the right
              // pipe at time "thisTime".
              case Left(v) => (lastJoined, Some((thisTime, v, lastJoined)))

              // Right(joinedV) means that we've received a new value
              // to use in the simulated realtime service described in
              // the comments above
              case Right(joined) => (Some(joined), None)
            }
        }.toTypedPipe

    for {
      // Now, get rid of residual state from the scanLeft above:
      (k, (_, optV)) <- joined

      // filter out every event that produced a Right(delta) above,
      // leaving only the leftJoin events that occurred above:
      (t, v, optJoined) <- optV
    } yield (t, (k, (v, optJoined)))
  }
}
