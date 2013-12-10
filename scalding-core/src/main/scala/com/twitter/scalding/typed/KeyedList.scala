/*
Copyright 2012 Twitter, Inc.

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

import com.twitter.algebird.{Semigroup, Ring, Aggregator}

import com.twitter.scalding._

/** Represents sharded lists of items of type T
 */
trait KeyedList[+K,+T] extends KeyedListLike[K,T] {
  type This[+K, +T] = KeyedList[K,T]
  // These are the fundamental operations
  def toTypedPipe : TypedPipe[(K, T)]
  /** Operate on a Stream[T] of all the values for each key at one time.
   * Avoid accumulating the whole list in memory if you can.  Prefer reduce.
   */
  def mapValueStream[V](smfn : Iterator[T] => Iterator[V]): KeyedList[K, V]
}
