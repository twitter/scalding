/*
Copyright 2015 Twitter, Inc.

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

package com.twitter.scalding.serialization

import java.io.{ ByteArrayInputStream, InputStream, OutputStream }
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

/**
 * In large-scale partitioning algorithms, we often use sorting.
 * This typeclass represents something we can efficiently serialize
 * with an added law: that we can (hopefully fast) compare the raw
 * data.
 */
trait OrderedSerialization[T] extends Ordering[T] with Serialization[T] {
  /**
   * This compares two InputStreams. After this call, the position in
   * the InputStreams is mutated to be the end of the record.
   */
  def compareBinary(a: InputStream, b: InputStream): OrderedSerialization.Result
}

object OrderedSerialization {
  /**
   * Represents the result of a comparison that might fail due
   * to an error deserializing
   */
  sealed trait Result {
    /**
     * Throws if the items cannot be compared
     */
    def unsafeToInt: Int
    def toTry: Try[Int]
  }
  /**
   * Create a Result from an Int.
   */
  def resultFrom(i: Int): Result =
    if (i > 0) Greater
    else if (i < 0) Less
    else Equal

  def resultFrom(t: Try[Int]): Result = t match {
    case Success(i) => resultFrom(i)
    case Failure(e) => CompareFailure(e)
  }

  final case class CompareFailure(ex: Throwable) extends Result {
    def unsafeToInt = throw ex
    def toTry = Failure(ex)
  }
  case object Greater extends Result {
    val unsafeToInt = 1
    val toTry = Success(unsafeToInt)
  }
  case object Equal extends Result {
    val unsafeToInt = 0
    val toTry = Success(unsafeToInt)
  }
  case object Less extends Result {
    val unsafeToInt = -1
    val toTry = Success(unsafeToInt)
  }

  def compare[T](a: T, b: T)(implicit ord: OrderedSerialization[T]): Int =
    ord.compare(a, b)

  def compareBinary[T](a: InputStream, b: InputStream)(implicit ord: OrderedSerialization[T]): Result =
    ord.compareBinary(a, b)

  def writeThenCompare[T](a: T, b: T)(implicit ordb: OrderedSerialization[T]): Result = {
    val abytes = Serialization.toBytes(a)
    val bbytes = Serialization.toBytes(b)
    val ain = new ByteArrayInputStream(abytes)
    val bin = new ByteArrayInputStream(bbytes)
    ordb.compareBinary(ain, bin)
  }

  /**
   * This is slow, but always an option. Avoid this if you can, especially for large items
   */
  def readThenCompare[T: OrderedSerialization](as: InputStream, bs: InputStream): Result = try resultFrom {
    val a = Serialization.read[T](as)
    val b = Serialization.read[T](bs)
    compare(a.get, b.get)
  } catch {
    case NonFatal(e) => CompareFailure(e)
  }

  private[this] def internalTransformer[T, U, V](packFn: T => U,
    unpackFn: U => V,
    presentFn: Try[V] => Try[T])(implicit otherOrdSer: OrderedSerialization[U]): OrderedSerialization[T] =
    {
      new OrderedSerialization[T] {
        private[this] var cache: (T, U) = null
        private[this] def packCache(t: T): U = {
          val readCache = cache
          if (readCache == null || readCache._1 != t) {
            val u = packFn(t)
            cache = (t, u)
            u
          } else {
            readCache._2
          }
        }

        override def hash(t: T) = otherOrdSer.hash(packCache(t))

        override def compareBinary(a: java.io.InputStream, b: java.io.InputStream): OrderedSerialization.Result =
          otherOrdSer.compareBinary(a, b)

        override def compare(x: T, y: T) =
          otherOrdSer.compare(packFn(x), packFn(y))

        override def read(in: InputStream): Try[T] =
          presentFn(otherOrdSer.read(in).map(unpackFn))

        override def write(out: OutputStream, t: T): Try[Unit] =
          otherOrdSer.write(out, packCache(t))

        override def staticSize: Option[Int] = otherOrdSer.staticSize

        override def dynamicSize(t: T): Option[Int] = otherOrdSer.dynamicSize(packCache(t))
      }
    }

  def viaTransform[T, U](
    packFn: T => U,
    unpackFn: U => T)(implicit otherOrdSer: OrderedSerialization[U]): OrderedSerialization[T] =
    internalTransformer[T, U, T](packFn, unpackFn, identity)

  def viaTryTransform[T, U](
    packFn: T => U,
    unpackFn: U => Try[T])(implicit otherOrdSer: OrderedSerialization[U]): OrderedSerialization[T] =
    internalTransformer[T, U, Try[T]](packFn, unpackFn, _.flatMap(identity))

  /**
   * The the serialized comparison matches the unserialized comparison
   */
  def compareBinaryMatchesCompare[T](implicit ordb: OrderedSerialization[T]): Law2[T] =
    Law2("compare(a, b) == compareBinary(aBin, bBin)",
      { (a: T, b: T) => resultFrom(ordb.compare(a, b)) == writeThenCompare(a, b) })

  /**
   * ordering must be transitive. If this is not so, sort-based partitioning
   * will be broken
   */
  def orderingTransitive[T](implicit ordb: OrderedSerialization[T]): Law3[T] =
    Law3("transitivity",
      { (a: T, b: T, c: T) =>
        if (ordb.lteq(a, b) && ordb.lteq(b, c)) { ordb.lteq(a, c) }
        else true
      })
  /**
   * ordering must be antisymmetric. If this is not so, sort-based partitioning
   * will be broken
   */
  def orderingAntisymmetry[T](implicit ordb: OrderedSerialization[T]): Law2[T] =
    Law2("antisymmetry",
      { (a: T, b: T) =>
        if (ordb.lteq(a, b) && ordb.lteq(b, a)) { ordb.equiv(a, b) }
        else true
      })
  /**
   * ordering must be total. If this is not so, sort-based partitioning
   * will be broken
   */
  def orderingTotality[T](implicit ordb: OrderedSerialization[T]): Law2[T] =
    Law2("totality", { (a: T, b: T) => (ordb.lteq(a, b) || ordb.lteq(b, a)) })

  def allLaws[T: OrderedSerialization]: Iterable[Law[T]] =
    Serialization.allLaws ++ List(compareBinaryMatchesCompare[T],
      orderingTransitive[T],
      orderingAntisymmetry[T],
      orderingTotality[T])
}

/**
 * This may be useful when a type is used deep in a tuple or case class, and in that case
 * the earlier comparators will have likely already done the work. Be aware that avoiding
 * deserialization on compare usually very helpful.
 *
 * Note: it is your responsibility that the hash in serialization is consistent
 * with the ordering (if equivalent in the ordering, the hash must match).
 */
final case class DeserializingOrderedSerialization[T](serialization: Serialization[T],
  ordering: Ordering[T]) extends OrderedSerialization[T] {

  final override def read(i: InputStream) = serialization.read(i)
  final override def write(o: OutputStream, t: T) = serialization.write(o, t)
  final override def hash(t: T) = serialization.hash(t)
  final override def compare(a: T, b: T) = ordering.compare(a, b)
  final override def compareBinary(a: InputStream, b: InputStream) =
    try OrderedSerialization.resultFrom {
      compare(read(a).get, read(b).get)
    }
    catch {
      case NonFatal(e) => OrderedSerialization.CompareFailure(e)
    }
  final override def staticSize = serialization.staticSize
  final override def dynamicSize(t: T) = serialization.dynamicSize(t)
}
