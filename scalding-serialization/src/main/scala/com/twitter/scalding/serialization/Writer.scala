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

import java.io.OutputStream

/**
 * This is a specialized typeclass to make it easier to implement Serializations.
 * The specialization *should* mean that there is no boxing and if the JIT
 * does its work, Writer should compose well (via collections, Tuple2, Option, Either)
 */
trait Writer[@specialized(Boolean, Byte, Short, Int, Long, Float, Double) -T] {
  def write(os: OutputStream, t: T): Unit
}

object Writer {
  import JavaStreamEnrichments._

  def write[@specialized(Boolean, Byte, Short, Int, Long, Float, Double) T](os: OutputStream,
    t: T)(implicit w: Writer[T]): Unit =
    w.write(os, t)
  /*
   * Instances below
   */
  implicit val unit: Writer[Unit] = new Writer[Unit] {
    def write(os: OutputStream, u: Unit) = ()
  }
  implicit val boolean: Writer[Boolean] = new Writer[Boolean] {
    def write(os: OutputStream, b: Boolean) = os.writeBoolean(b)
  }
  implicit val byte: Writer[Byte] = new Writer[Byte] {
    def write(os: OutputStream, b: Byte) = os.write(b)
  }
  implicit val short: Writer[Short] = new Writer[Short] {
    def write(os: OutputStream, s: Short) = os.writeShort(s)
  }
  implicit val int: Writer[Int] = new Writer[Int] {
    def write(os: OutputStream, s: Int) = os.writeInt(s)
  }
  implicit val long: Writer[Long] = new Writer[Long] {
    def write(os: OutputStream, s: Long) = os.writeLong(s)
  }
  implicit val float: Writer[Float] = new Writer[Float] {
    def write(os: OutputStream, s: Float) = os.writeFloat(s)
  }
  implicit val double: Writer[Double] = new Writer[Double] {
    def write(os: OutputStream, s: Double) = os.writeDouble(s)
  }
  implicit val string: Writer[String] = new Writer[String] {
    def write(os: OutputStream, s: String) = {
      val bytes = s.getBytes("UTF-8")
      os.writePosVarInt(bytes.length)
      os.writeBytes(bytes)
    }
  }

  implicit def option[T: Writer]: Writer[Option[T]] = new Writer[Option[T]] {
    val w = implicitly[Writer[T]]
    // Don't use pattern matching in a performance-critical section
    @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
    def write(os: OutputStream, t: Option[T]) =
      if (t.isDefined) {
        os.write(1: Byte)
        w.write(os, t.get)
      } else os.write(0: Byte)
  }

  implicit def either[L: Writer, R: Writer]: Writer[Either[L, R]] = new Writer[Either[L, R]] {
    val lw = implicitly[Writer[L]]
    val rw = implicitly[Writer[R]]
    def write(os: OutputStream, e: Either[L, R]) = e match {
      case Left(l) =>
        os.write(0: Byte)
        lw.write(os, l)
      case Right(r) =>
        os.write(1: Byte)
        rw.write(os, r)
    }
  }

  implicit def tuple2[T1: Writer, T2: Writer]: Writer[(T1, T2)] = new Writer[(T1, T2)] {
    val w1 = implicitly[Writer[T1]]
    val w2 = implicitly[Writer[T2]]
    def write(os: OutputStream, tup: (T1, T2)) = {
      w1.write(os, tup._1)
      w2.write(os, tup._2)
    }
  }

  implicit def array[@specialized(Boolean, Byte, Short, Int, Long, Float, Double) T: Writer]: Writer[Array[T]] =
    new Writer[Array[T]] {
      val writerT = implicitly[Writer[T]]
      def write(os: OutputStream, a: Array[T]) = {
        val size = a.length
        os.writePosVarInt(size)
        @annotation.tailrec
        def go(p: Int): Unit =
          if (p == size) ()
          else { writerT.write(os, a(p)); go(p + 1) }

        go(0)
      }
    }

  // Scala has problems with this being implicit
  def collection[T: Writer, C <: Iterable[T]]: Writer[C] = new Writer[C] {
    val writerT = implicitly[Writer[T]]
    def write(os: OutputStream, c: C) = {
      val size = c.size
      os.writePosVarInt(size)
      c.foreach { t: T =>
        writerT.write(os, t)
      }
    }
  }
}

