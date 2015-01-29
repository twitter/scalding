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

// Be careful using this, the product/array or similar will attempt to call system hash codes.
import scala.util.hashing.MurmurHash3
/**
 * This is a specialized typeclass to make it easier to implement Serializations.
 * The specialization *should* mean that there is no boxing and if the JIT
 * does its work, Hasher should compose well (via collections, Tuple2, Option, Either)
 */
trait Hasher[@specialized(Boolean, Byte, Short, Int, Long, Float, Double) -T] {
  @inline
  def hash(i: T): Int
}

object Hasher {
  import MurmerHashUtils._
  final val seed = 0xf7ca7fd2

  @inline
  def hash[@specialized(Boolean, Byte, Short, Int, Long, Float, Double) T](
    i: T)(implicit h: Hasher[T]): Int = h.hash(i)

  /*
   * Instances below
   */
  implicit val unit: Hasher[Unit] = new Hasher[Unit] {
    @inline
    def hash(i: Unit) = 0
  }
  implicit val boolean: Hasher[Boolean] = new Hasher[Boolean] {
    // Here we use the two largest mersenne primes
    @inline
    def hash(i: Boolean) = if (i) Int.MaxValue else ((1 << 19) - 1)
  }
  implicit val byte: Hasher[Byte] = new Hasher[Byte] {
    @inline
    def hash(i: Byte) = hashInt(i.toInt)
  }
  implicit val short: Hasher[Short] = new Hasher[Short] {
    @inline
    def hash(i: Short) = hashInt(i.toInt)
  }

  implicit val int: Hasher[Int] = new Hasher[Int] {
    @inline
    def hash(i: Int) = hashInt(i)
  }

  // java way to refer to int, alias in naming
  val integer = int

  implicit val long: Hasher[Long] = new Hasher[Long] {
    @inline
    def hash(i: Long) = hashLong(i)
  }

  implicit val float: Hasher[Float] = new Hasher[Float] {
    @inline
    def hash(i: Float) = hashInt(java.lang.Float.valueOf(i).intValue)
  }
  implicit val double: Hasher[Double] = new Hasher[Double] {
    @inline
    def hash(i: Double) = hashLong(i.longValue)
  }
  implicit val string: Hasher[String] = new Hasher[String] {
    @inline
    def hash(i: String) = MurmurHash3.stringHash(i)
  }
}
