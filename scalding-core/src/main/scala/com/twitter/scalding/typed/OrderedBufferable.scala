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

import com.twitter.bijection.{ Bijection, Bufferable, Injection }
import java.io.Serializable
import java.nio.ByteBuffer
import java.util.Comparator
import cascading.tuple.{ Hasher, StreamComparator }
import cascading.tuple.hadoop.io.BufferedInputStream

import scala.util.{ Failure, Success, Try }

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

case class BijectedOrderedBufferable[A, B](bij: Bijection[A, B],
  ord: OrderedBufferable[B]) extends OrderedBufferable[A] {
  override def compare(a: A, b: A) = ord.compare(bij(a), bij(b))
  override def hash(a: A) = ord.hash(bij(a))
  override def compareBinary(a: ByteBuffer, b: ByteBuffer) = ord.compareBinary(a, b)
  override def get(in: ByteBuffer): scala.util.Try[(ByteBuffer, A)] =
    ord.get(in).map { case (buf, b) => (buf, bij.invert(b)) }
  override def put(out: ByteBuffer, a: A): ByteBuffer =
    ord.put(out, bij(a))
}

case class InjectedOrderedBufferable[A, B](inj: Injection[A, B],
  ord: OrderedBufferable[B]) extends OrderedBufferable[A] {
  override def compare(a: A, b: A) = ord.compare(inj(a), inj(b))
  override def hash(a: A) = ord.hash(inj(a))
  override def compareBinary(a: ByteBuffer, b: ByteBuffer) = ord.compareBinary(a, b)
  override def get(in: ByteBuffer): scala.util.Try[(ByteBuffer, A)] =
    for {
      bufb <- ord.get(in)
      (buf, b) = bufb // avoid .filter in for, by using two steps here. dump scalac.
      a <- inj.invert(b)
    } yield (buf, a)

  override def put(out: ByteBuffer, a: A): ByteBuffer =
    ord.put(out, inj(a))
}

case class OrderedBufferable2[A, B](ordA: OrderedBufferable[A], ordB: OrderedBufferable[B]) extends OrderedBufferable[(A, B)] {
  private val MAX_PRIME = Int.MaxValue // turns out MaxValue is a prime, which we want below
  override def compare(x: (A, B), y: (A, B)) = {
    val ca = ordA.compare(x._1, y._1)
    if (ca != 0) ca
    else ordB.compare(x._2, y._2)
  }
  override def hash(x: (A, B)) = ordA.hash(x._1) + MAX_PRIME * ordB.hash(x._2)
  override def compareBinary(a: ByteBuffer, b: ByteBuffer) = {
    // This mutates the buffers and advances them. Only keep reading if they are different
    ordA.compareBinary(a, b) match {
      case OrderedBufferable.Equal => OrderedBufferable.Equal
      case f @ OrderedBufferable.CompareFailure(_) => f
      case _ => ordB.compareBinary(a, b)
    }
  }
  override def get(in: ByteBuffer): scala.util.Try[(ByteBuffer, (A, B))] =
    ordA.get(in) match {
      case Success((ba, a)) =>
        ordB.get(ba) match {
          case Success((bb, b)) => Success(bb, (a, b))
          case Failure(e) => Failure(e)
        }
      case Failure(e) => Failure(e)
    }

  override def put(out: ByteBuffer, a: (A, B)): ByteBuffer =
    ordB.put(ordA.put(out, a._1), a._2)
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
    def toByteBuffer(bis: BufferedInputStream): ByteBuffer = {
      // This, despite what the name implies, is not the number
      // of bytes, but the absolute position of the strict upper bound
      val upperBoundPosition = bis.getLength
      val length = upperBoundPosition - bis.getPosition
      ByteBuffer.wrap(bis.getBuffer, bis.getPosition, length)
    }

    val bba = toByteBuffer(a)
    val bbb = toByteBuffer(b)
    val res = ob.compareBinary(bba, bbb).unsafeToInt
    a.skip(bba.position - a.getPosition)
    b.skip(bbb.position - b.getPosition)
    res
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
  /**
   * Create a Result from an Int.
   */
  def resultFrom(i: Int): Result =
    if (i > 0) Greater
    else if (i < 0) Less
    else Equal

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
