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
import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream, Serializable }
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
trait OrderedBinary[T] extends Ordering[T] with Serializable {
  def get(from: InputStream): Try[T]
  def put(into: OutputStream, t: T): Try[Unit]
  /**
   * This compares two InputStreams. After this call, the position in
   * the InputStreams is mutated to be the end of the record.
   */
  def compareBinary(a: InputStream, b: InputStream): OrderedBinary.Result
  /**
   * This needs to be consistent with the ordering: equal objects must hash the same
   * Hadoop always gets the hashcode before serializing
   */
  def hash(t: T): Int
}

case class BijectedOrderedBinary[A, B](bij: Bijection[A, B],
  ord: OrderedBinary[B]) extends OrderedBinary[A] {
  override def compare(a: A, b: A) = ord.compare(bij(a), bij(b))
  override def hash(a: A) = ord.hash(bij(a))
  override def compareBinary(a: InputStream, b: InputStream) = ord.compareBinary(a, b)
  override def get(in: InputStream): Try[A] =
    ord.get(in).map(bij.invert)
  override def put(out: OutputStream, a: A): Try[Unit] =
    ord.put(out, bij(a))
}

case class InjectedOrderedBinary[A, B](inj: Injection[A, B],
  ord: OrderedBinary[B]) extends OrderedBinary[A] {
  override def compare(a: A, b: A) = ord.compare(inj(a), inj(b))
  override def hash(a: A) = ord.hash(inj(a))
  override def compareBinary(a: InputStream, b: InputStream) = ord.compareBinary(a, b)
  override def get(in: InputStream): Try[A] =
    for {
      b <- ord.get(in)
      a <- inj.invert(b)
    } yield a

  override def put(out: OutputStream, a: A): Try[Unit] =
    ord.put(out, inj(a))
}

case class OrderedBinary2[A, B](ordA: OrderedBinary[A], ordB: OrderedBinary[B]) extends OrderedBinary[(A, B)] {
  private val MAX_PRIME = Int.MaxValue // turns out MaxValue is a prime, which we want below
  override def compare(x: (A, B), y: (A, B)) = {
    val ca = ordA.compare(x._1, y._1)
    if (ca != 0) ca
    else ordB.compare(x._2, y._2)
  }
  override def hash(x: (A, B)) = ordA.hash(x._1) + MAX_PRIME * ordB.hash(x._2)
  override def compareBinary(a: InputStream, b: InputStream) = {
    // This mutates the buffers and advances them. Only keep reading if they are different
    val cA = ordA.compareBinary(a, b)
    // we have to read the second ones to skip
    val cB = ordB.compareBinary(a, b)
    cA match {
      case OrderedBinary.Equal => cB
      case f @ OrderedBinary.CompareFailure(_) => f
      case _ => cA // the first is not equal
    }
  }

  override def get(in: InputStream): Try[(A, B)] =
    (ordA.get(in), ordB.get(in)) match {
      case (Success(a), Success(b)) => Success((a, b))
      case (Failure(e), _) => Failure(e)
      case (_, Failure(e)) => Failure(e)
    }

  override def put(out: OutputStream, a: (A, B)): Try[Unit] = {
    val resA = ordA.put(out, a._1)
    if (resA.isSuccess) ordB.put(out, a._2)
    else resA
  }
}

/**
 * This is the type that should be fed to cascading to enable binary comparators
 */
class CascadingBinaryComparator[T](ob: OrderedBinary[T]) extends Comparator[T]
  with StreamComparator[BufferedInputStream]
  with Hasher[T]
  with Serializable {

  override def compare(a: T, b: T) = ob.compare(a, b)
  override def hashCode(t: T) = ob.hash(t)
  override def compare(a: BufferedInputStream, b: BufferedInputStream) =
    ob.compareBinary(a, b).unsafeToInt
}

object OrderedBinary {

  /**
   * This is a constant for us to reuse in OrderedBinary.put
   */
  val successUnit: Try[Unit] = Success(())

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

  def serializeThenCompare[T](a: T, b: T)(implicit ordb: OrderedBinary[T]): Int = {
    val aout = new ByteArrayOutputStream()
    val bout = new ByteArrayOutputStream()
    (for {
      _ <- ordb.put(aout, a)
      _ <- ordb.put(bout, a)
    } yield ()).get // this will throw if we can't serialize

    val ain = new ByteArrayInputStream(aout.toByteArray)
    val bin = new ByteArrayInputStream(bout.toByteArray)
    ordb.compareBinary(ain, bin).unsafeToInt
  }

  /**
   * The the serialized comparison matches the unserialized comparison
   */
  def law1[T](implicit ordb: OrderedBinary[T]): (T, T) => Boolean = { (a: T, b: T) =>
    ordb.compare(a, b) == serializeThenCompare(a, b)
  }
  /**
   * Two items are not equal or the hash codes match
   */
  def law2[T](implicit ordb: OrderedBinary[T]): (T, T) => Boolean = { (a: T, b: T) =>
    (serializeThenCompare(a, b) != 0) || (ordb.hash(a) == ordb.hash(b))
  }
}
