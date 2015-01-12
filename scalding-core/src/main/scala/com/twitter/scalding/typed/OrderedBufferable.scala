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
import scala.util.{ Success, Try }

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
    def clamp(i: Int) = if (i > 0) 1 else if (i < 0) -1 else 0
    clamp(ordb.compare(a, b)) == serializeThenCompare(a, b)
  }
  /**
   * Two items are not equal or the hash codes match
   */
  def law2[T](implicit ordb: OrderedBufferable[T]): (T, T) => Boolean = { (a: T, b: T) =>
    (!ordb.equiv(a, b)) || (ordb.hash(a) == ordb.hash(b))
  }
  /*
   * put followed by a get gives an equal item
   */
  def law3[T](implicit ordb: OrderedBufferable[T]): (T) => Boolean = { (t: T) =>
    val bb = Bufferable.reallocatingPut(ByteBuffer.allocate(128))(ordb.put(_, t))
    bb.position(0)
    ordb.get(bb) match {
      case Success((_, t2)) if ordb.equiv(t, t2) => true
      case _ => false
    }
  }

  /**
   * Use a varint encoding to read a size
   */
  def getSize(b: ByteBuffer): Int = {
    val b1 = b.get
    def fromByte(b: Byte): Int = if (b < 0) b + (1 << 8) else b.toInt
    def fromShort(s: Short): Int = if (s < 0) s + (1 << 16) else s.toInt
    if (b1 != (-1: Byte)) fromByte(b1) else {
      val s1 = b.getShort
      if (s1 != (-1: Short)) fromShort(s1) else b.getInt
    }
  }
  def putSize(b: ByteBuffer, s: Int): Unit = {
    require(s >= 0, s"size must be non-negative: ${s}")
    if (s < ((1 << 8) - 1)) b.put(s.toByte)
    else {
      b.put(-1: Byte)
      if (s < ((1 << 16) - 1)) b.putShort(s.toShort)
      else {
        b.putShort(-1: Short)
        b.putInt(s)
      }
    }
  }

  final def unsignedLongCompare(a: Long, b: Long): Int = if (a == b) 0 else {
    // We only get into this block when the a != b, so it has to be the last
    // block
    val firstBitXor = (a ^ b) & (1L << 63)
    // If both are on the same side of zero, normal compare works
    if (firstBitXor == 0) java.lang.Long.compare(a, b)
    else if (b >= 0) 1
    else -1
  }
  final def unsignedIntCompare(a: Int, b: Int): Int = if (a == b) 0 else {
    val firstBitXor = (a ^ b) & (1 << 31)
    // If both are on the same side of zero, normal compare works
    if (firstBitXor == 0) Integer.compare(a, b)
    else if (b >= 0) 1
    else -1
  }
  final def unsignedShortCompare(a: Short, b: Short): Int = if (a == b) 0 else {
    // We have to convert to bytes to Int on JVM to do
    // anything anyway, so might as well compare in that space
    def fromShort(x: Short): Int = if (x < 0) x + (1 << 16) else x.toInt
    Integer.compare(fromShort(a), fromShort(b))
  }
  final def unsignedByteCompare(a: Byte, b: Byte): Int = if (a == b) 0 else {
    // We have to convert to bytes to Int on JVM to do
    // anything anyway, so might as well compare in that space
    def fromByte(x: Byte): Int = if (x < 0) x + (1 << 8) else x.toInt
    Integer.compare(fromByte(a), fromByte(b))
  }
}

class StringOrderedBufferable extends OrderedBufferable[String] {
  override def hash(s: String) = s.hashCode
  override def compare(a: String, b: String) = a.compareTo(b)
  override def get(b: ByteBuffer) = Try {
    val toMutate = b.duplicate
    val byteSize = OrderedBufferable.getSize(toMutate)
    val byteString = new Array[Byte](byteSize)
    toMutate.get(byteString)
    (toMutate, new String(byteString, "UTF-8"))
  }
  override def put(b: ByteBuffer, s: String): ByteBuffer = {
    val toMutate = b.duplicate
    val bytes = s.getBytes("UTF-8")
    OrderedBufferable.putSize(toMutate, bytes.length)
    toMutate.put(bytes)
    toMutate
  }
  override def compareBinary(a: ByteBuffer, b: ByteBuffer) = try OrderedBufferable.resultFrom {
    val sizeA = OrderedBufferable.getSize(a)
    val posA = a.position
    val sizeB = OrderedBufferable.getSize(b)
    val posB = b.position

    def getIntResult: Int = {
      val toCheck = math.min(sizeA, sizeB)
      // we can check longs at a time this way:
      val longs = toCheck / 8
      val remaining = (toCheck - 8 * longs)
      val ints = (remaining >= 4)
      val bytes = remaining - (if (ints) 4 else 0)

      @annotation.tailrec
      def compareLong(count: Int): Int =
        if (count == 0) 0
        else {
          val cmp = OrderedBufferable.unsignedLongCompare(a.getLong, b.getLong)
          if (cmp == 0) compareLong(count - 1)
          else cmp
        }

      /*
       * This algorithm only works if count in {0, 1, 2, 3}. Since we only
       * call it that way below it is safe.
       */
      def compareBytes(count: Int): Int =
        if ((count & 0x10) == 0x10) {
          // there are 2 or 3 bytes to read
          val cmp = OrderedBufferable.unsignedShortCompare(a.getShort, b.getShort)
          if (cmp != 0) cmp
          else if (count == 3) OrderedBufferable.unsignedByteCompare(a.get, b.get)
          else 0
        } else {
          // there are 0 or 1 bytes to read
          if (count == 0) 0
          else OrderedBufferable.unsignedByteCompare(a.get, b.get)
        }

      val lc = compareLong(longs)
      if (lc != 0) lc
      else {
        val ic = if (ints) OrderedBufferable.unsignedIntCompare(a.getInt, b.getInt) else 0
        if (ic != 0) ic
        else {
          val bc = compareBytes(bytes)
          if (bc != 0) bc
          else {
            // the size is the fallback when the prefixes match:
            Integer.compare(sizeA, sizeB)
          }
        }
      }
    }

    /**
     * We need to advance the buffer past all these bytes
     */
    val res = getIntResult
    a.position(posA + sizeA)
    b.position(posB + sizeB)
    res
  } catch {
    case e: Throwable => OrderedBufferable.CompareFailure(e)
  }
}
