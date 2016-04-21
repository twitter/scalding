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

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream, Serializable }

import scala.util.{ Success, Try }
import scala.util.hashing.Hashing

/**
 * This is a base Input/OutputStream-based serialization typeclass
 * This is useful for value serialization in hadoop when we don't
 * need to do key sorting for partitioning.
 *
 * This serialization typeclass must serialize equivalent objects
 * identically to be lawful. Serialization should be the same
 * on all JVMs run at any time, in other words, Serialization is a
 * pure function. Given that constraint, we can always
 * get an Equiv and Hashing from a Serialization (by doing byte-wise
 * equivalence or byte-wise hashing).
 *
 * A serialization always gives a hash because one can just
 * serialize and then hash the bytes. You might prefer another
 * implementation. This must satisfy:
 *   (!equiv(a, b)) || (hash(a) == hash(b))
 */
trait Serialization[T] extends Equiv[T] with Hashing[T] with Serializable {
  def read(in: InputStream): Try[T]
  def write(out: OutputStream, t: T): Try[Unit]
  /**
   * If all items have a static size, this returns Some, else None
   * NOTE: lawful implementations that return Some here much return
   * Some on dynamicSize so callers don't need to check both when
   * they have an instance.
   */
  def staticSize: Option[Int]
  /**
   * returns Some if the size is cheap to calculate.
   * otherwise the caller should just serialize into an ByteArrayOutputStream
   */
  def dynamicSize(t: T): Option[Int]
}

/**
 * In order to cache Serializations having equality and hashes can be useful.
 * Extend this trait when those two properties can be satisfied
 */
trait EquivSerialization[T] extends Serialization[T]

object Serialization {
  import JavaStreamEnrichments._
  /**
   * This is a constant for us to reuse in Serialization.write
   */
  val successUnit: Try[Unit] = Success(())

  def equiv[T](a: T, b: T)(implicit ser: Serialization[T]): Boolean =
    ser.equiv(a, b)

  def hash[T](t: T)(implicit ser: Serialization[T]): Int =
    ser.hash(t)

  def read[T](in: InputStream)(implicit ser: Serialization[T]): Try[T] =
    ser.read(in)

  def write[T](out: OutputStream, t: T)(implicit ser: Serialization[T]): Try[Unit] =
    ser.write(out, t)

  def toBytes[T](t: T)(implicit ser: Serialization[T]): Array[Byte] = {
    ser.dynamicSize(t) match {
      case None =>
        val baos = new ByteArrayOutputStream
        write(baos, t).get // this should only throw on OOM
        baos.toByteArray
      case Some(size) =>
        // If we know the size, we can just write directly into a fixed
        // size byte array
        val bytes = new Array[Byte](size)
        val os = bytes.wrapAsOutputStream
        write(os, t).get // this should only throw on OOM
        bytes
    }
  }

  def fromBytes[T: Serialization](b: Array[Byte]): Try[T] =
    read(new ByteArrayInputStream(b))

  /**
   * This copies more than needed, but it is only for testing
   */
  private def roundTrip[T](t: T)(implicit ser: Serialization[T]): T = {
    val baos = new ByteArrayOutputStream
    ser.write(baos, t).get // should never throw on a ByteArrayOutputStream
    ser.read(baos.toInputStream).get
  }

  /**
   * Do these two items write equivalently?
   */
  def writeEquiv[T: Serialization](a: T, b: T): Boolean =
    java.util.Arrays.equals(toBytes(a), toBytes(b))

  /**
   * write followed by read should give an equivalent T
   *
   * This is a law that serialization must follow. It is here for
   * documentation and for use within tests without any dependence on
   * specific test frameworks.
   *
   * forAll(roundTripLaw[T]) in a valid test in scalacheck style
   */
  def roundTripLaw[T: Serialization]: Law1[T] =
    Law1("roundTrip", { (t: T) => equiv(roundTrip(t), t) })

  /**
   * If two items are equal, they should serialize byte for byte equivalently
   */
  def serializationIsEquivalence[T: Serialization]: Law2[T] =
    Law2("equiv(a, b) == (write(a) == write(b))", { (t1: T, t2: T) =>
      equiv(t1, t2) == writeEquiv(t1, t2)
    })

  def hashCodeImpliesEquality[T: Serialization]: Law2[T] =
    Law2("equiv(a, b) => hash(a) == hash(b)", { (t1: T, t2: T) =>
      !equiv(t1, t2) || (hash(t1) == hash(t2))
    })

  def reflexivity[T: Serialization]: Law1[T] =
    Law1("equiv(a, a) == true", { (t1: T) => equiv(t1, t1) })

  /**
   * The sizes must match and be correct if they are present
   */
  def sizeLaw[T: Serialization]: Law1[T] =
    Law1("staticSize.orElse(dynamicSize(t)).map { _ == toBytes(t).length }",
      { (t: T) =>
        val ser = implicitly[Serialization[T]]
        (ser.staticSize, ser.dynamicSize(t)) match {
          case (Some(s), Some(d)) if d == s => toBytes(t).length == s
          case (Some(s), _) => false // if static exists it must match dynamic
          case (None, Some(d)) => toBytes(t).length == d
          case (None, None) => true // can't tell
        }
      })

  def transitivity[T: Serialization]: Law3[T] =
    Law3("equiv(a, b) && equiv(b, c) => equiv(a, c)",
      { (t1: T, t2: T, t3: T) =>
        !(equiv(t1, t2) && equiv(t2, t3)) || equiv(t1, t3)
      })

  def allLaws[T: Serialization]: Iterable[Law[T]] =
    List(roundTripLaw,
      serializationIsEquivalence,
      hashCodeImpliesEquality,
      reflexivity,
      sizeLaw,
      transitivity)
}
