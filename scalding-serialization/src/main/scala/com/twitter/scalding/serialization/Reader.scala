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

import java.io.InputStream
import scala.reflect.ClassTag
import scala.collection.generic.CanBuildFrom

/**
 * This is a specialized typeclass to make it easier to implement Serializations.
 * The specialization *should* mean that there is no boxing and if the JIT
 * does its work, Reader should compose well (via collections, Tuple2, Option, Either)
 */
trait Reader[@specialized(Boolean, Byte, Short, Int, Long, Float, Double) +T] {
  def read(is: InputStream): T
}

object Reader {
  import JavaStreamEnrichments._

  def read[@specialized(Boolean, Byte, Short, Int, Long, Float, Double) T](
    is: InputStream)(implicit r: Reader[T]): T = r.read(is)
  /*
   * Instances below
   */
  implicit val unit: Reader[Unit] = new Reader[Unit] {
    def read(is: InputStream) = ()
  }
  implicit val boolean: Reader[Boolean] = new Reader[Boolean] {
    def read(is: InputStream) = is.readBoolean
  }
  implicit val byte: Reader[Byte] = new Reader[Byte] {
    def read(is: InputStream) = is.readByte
  }
  implicit val short: Reader[Short] = new Reader[Short] {
    def read(is: InputStream) = is.readShort
  }
  implicit val int: Reader[Int] = new Reader[Int] {
    def read(is: InputStream) = is.readInt
  }
  implicit val long: Reader[Long] = new Reader[Long] {
    def read(is: InputStream) = is.readLong
  }
  implicit val float: Reader[Float] = new Reader[Float] {
    def read(is: InputStream) = is.readFloat
  }
  implicit val double: Reader[Double] = new Reader[Double] {
    def read(is: InputStream) = is.readDouble
  }
  implicit val string: Reader[String] = new Reader[String] {
    def read(is: InputStream) = {
      val size = is.readPosVarInt
      val bytes = new Array[Byte](size)
      is.readFully(bytes)
      new String(bytes, "UTF-8")
    }
  }

  implicit def option[T: Reader]: Reader[Option[T]] = new Reader[Option[T]] {
    val r = implicitly[Reader[T]]
    def read(is: InputStream) =
      if (is.readByte == (0: Byte)) None
      else Some(r.read(is))
  }

  implicit def either[L: Reader, R: Reader]: Reader[Either[L, R]] = new Reader[Either[L, R]] {
    val lRead = implicitly[Reader[L]]
    val rRead = implicitly[Reader[R]]
    def read(is: InputStream) =
      if (is.readByte == (0: Byte)) Left(lRead.read(is))
      else Right(rRead.read(is))
  }

  implicit def tuple2[T1: Reader, T2: Reader]: Reader[(T1, T2)] = new Reader[(T1, T2)] {
    val r1 = implicitly[Reader[T1]]
    val r2 = implicitly[Reader[T2]]
    def read(is: InputStream) = (r1.read(is), r2.read(is))
  }

  implicit def array[@specialized(Boolean, Byte, Short, Int, Long, Float, Double) T: Reader: ClassTag]: Reader[Array[T]] =
    new Reader[Array[T]] {
      val readerT = implicitly[Reader[T]]
      def read(is: InputStream) = {
        val size = is.readPosVarInt
        val res = new Array[T](size)
        @annotation.tailrec
        def go(p: Int): Unit =
          if (p == size) ()
          else {
            res(p) = readerT.read(is)
            go(p + 1)
          }
        go(0)
        res
      }
    }

  // Scala seems to have issues with this being implicit
  def collection[T: Reader, C](implicit cbf: CanBuildFrom[Nothing, T, C]): Reader[C] = new Reader[C] {
    val readerT = implicitly[Reader[T]]
    def read(is: InputStream): C = {
      val builder = cbf()
      val size = is.readPosVarInt
      builder.sizeHint(size)
      @annotation.tailrec
      def go(idx: Int): Unit =
        if (idx == size) ()
        else {
          builder += readerT.read(is)
          go(idx + 1)
        }

      go(0)
      builder.result
    }
  }
}
