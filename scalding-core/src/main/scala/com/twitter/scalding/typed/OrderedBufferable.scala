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

import com.twitter.bijection.Bufferable

import java.nio.ByteBuffer

/**
 * In large-scale partitioning algorithms, we often use sorting.
 * This typeclass represents something we can efficiently serialize
 * with an added law: that we can (hopefully fast) compare the raw
 * data.
 */
trait OrderedBufferable[T] extends Bufferable[T] with Ordering[T] {
  /**
   * This compares two ByteBuffers. After this call, the position in
   * the ByteBuffers is mutated. If you don't want this behavior
   * call duplicate before you pass in (which copies only the position, not the data)
   */
  def compareBinary(a: ByteBuffer, b: ByteBuffer): OrderedBufferable.Result
  def hash(t: T): Int
  /**
   * This hashes a ByteBuffer. Should be the same as hash(get(buf)).
   * After this call, the position in the ByteBuffer is mutated. If you don't want
   * this behavior call duplicate before you pass in (which copies only the
   * position, not the data)
   */
  def hashBinary(buf: ByteBuffer): OrderedBufferable.HashResult
}

object OrderedBufferable {
  /**
   * Represents the result of a comparison that might fail due
   * to an error deserializing
   */
  sealed trait Result {
    /**
     * Throws if the items cannot be compared
     */
    def unsafeToInt: Int
  }
  final case class CompareFailure(ex: Throwable) extends Result { def unsafeToInt = throw ex }
  case object Greater extends Result { val unsafeToInt = 1 }
  case object Equal extends Result { val unsafeToInt = 0 }
  case object Less extends Result { val unsafeToInt = -1 }

  sealed trait HashResult {
    /**
     * may throw if the ByteBuffer cannot be deserialized
     */
    def unsafeToInt: Int
  }
  final case class HashFailure(ex: Throwable) extends HashResult { def unsafeToInt = throw ex }
  final case class HashValue(override val unsafeToInt: Int) extends HashResult

  /**
   * The the serialized comparison matches the unserialized comparison
   */
  def law1[T](implicit ordb: OrderedBufferable[T]): (T, T) => Boolean = { (a: T, b: T) =>
    ordb.compare(a, b) == {
      val bb1 = Bufferable.reallocatingPut(ByteBuffer.allocate(128)) { ordb.put(_, a) }
      bb1.position(0)
      val bb2 = Bufferable.reallocatingPut(ByteBuffer.allocate(128)) { ordb.put(_, b) }
      bb2.position(0)
      ordb.compareBinary(bb1, bb2).unsafeToInt
    }
  }
  /**
   * Two items are not equal or the hash codes match
   */
  def law2[T](implicit ordb: OrderedBufferable[T]): (T, T) => Boolean = { (a: T, b: T) =>
    (ordb.compare(a, b) != 0) || (ordb.hash(a) == ordb.hash(b))
  }
  /**
   * binary and in-memory hash codes match
   */
  def law3[T](implicit ordb: OrderedBufferable[T]): T => Boolean = { (t: T) =>
    ordb.hash(t) == {
      val bb = Bufferable.reallocatingPut(ByteBuffer.allocate(128)) { ordb.put(_, t) }
      bb.position(0)
      ordb.hashBinary(bb).unsafeToInt
    }
  }
}
