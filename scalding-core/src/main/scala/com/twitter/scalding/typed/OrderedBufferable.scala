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
import java.io.Serializable
import java.nio.ByteBuffer
import java.util.Comparator
import cascading.tuple.{ Hasher, StreamComparator }
import cascading.tuple.hadoop.io.BufferedInputStream

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
  /**
   * This needs to be consistent with the ordering: equal objects must hash the same
   * Hadoop always gets the hashcode before serializing
   */
  def hash(t: T): Int
}

/**
 * This is the type that should be fed to cascading to enable binary comparators
 */
class CascadingBinaryComparator[T](ob: OrderedBufferable[T]) extends Comparator[T]
  with StreamComparator[BufferedInputStream]
  with Hasher[T]
  with Serializable {

  override def compare(a: T, b: T) = ob.compare(a, b)
  override def hashCode(t: T) = ob.hash(t)
  override def compare(a: BufferedInputStream, b: BufferedInputStream) = {
    def toByteBuffer(bis: BufferedInputStream): ByteBuffer =
      ByteBuffer.wrap(bis.getBuffer, bis.getPosition, bis.getLength)

    ob.compareBinary(toByteBuffer(a), toByteBuffer(b)).unsafeToInt
  }
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

  def serializeThenCompare[T](a: T, b: T)(implicit ordb: OrderedBufferable[T]): Int = {
    val bb1 = Bufferable.reallocatingPut(ByteBuffer.allocate(128)) { ordb.put(_, a) }
    bb1.position(0)
    val bb2 = Bufferable.reallocatingPut(ByteBuffer.allocate(128)) { ordb.put(_, b) }
    bb2.position(0)
    ordb.compareBinary(bb1, bb2).unsafeToInt
  }

  /**
   * The the serialized comparison matches the unserialized comparison
   */
  def law1[T](implicit ordb: OrderedBufferable[T]): (T, T) => Boolean = { (a: T, b: T) =>
    ordb.compare(a, b) == serializeThenCompare(a, b)
  }
  /**
   * Two items are not equal or the hash codes match
   */
  def law2[T](implicit ordb: OrderedBufferable[T]): (T, T) => Boolean = { (a: T, b: T) =>
    (serializeThenCompare(a, b) != 0) || (ordb.hash(a) == ordb.hash(b))
  }
}
