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
package com.twitter.scalding.serialization.provided

import com.twitter.scalding.serialization.JavaStreamEnrichments._

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream }
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal
import com.twitter.scalding.serialization._

object UnsafeOrderedSerialization {
  def apply[T](ordSer: OrderedSerialization[T]): UnsafeOrderedSerialization[T] = ordSer match {
    case us: UnsafeOrderedSerialization[T] => us
    case o =>
      new UnsafeOrderedSerialization[T] {
        def unsafeWrite(out: OutputStream, t: T): Unit = ordSer.write(out, t).get
        def unsafeRead(in: InputStream): T = ordSer.read(in).get
        def unsafeSwitchingCompareBinaryNoConsume(inputStreamA: InputStream, inputStreamB: InputStream, shouldConsume: Boolean): Int =
          ordSer.compareBinary(inputStreamA, inputStreamB).unsafeToInt
        def hash(t: T) = ordSer.hash(t)
        def compare(a: T, b: T) = ordSer.compare(a, b)
        @inline def staticSize: Option[Int] = ordSer.staticSize
        def dynamicSize(t: T): Option[Int] = ordSer.dynamicSize(t)
      }
  }
}

abstract class UnsafeOrderedSerialization[T] extends OrderedSerialization[T] {
  // This will write out the interior data as a blob with no prepended length
  // This means binary compare cannot skip on this data.
  // However the contract remains that one should be able to _read_ the data
  // back out again.
  def unsafeWrite(out: OutputStream, t: T): Unit
  // This is an internal read method that matches the unsafe write
  // importantly any outer length header added to enable skipping isn't included here
  def unsafeRead(in: InputStream): T

  // This is an inner binary compare that the user must supply
  def unsafeSwitchingCompareBinaryNoConsume(inputStreamA: InputStream, inputStreamB: InputStream, shouldConsume: Boolean): Int

  // This is the public write method, if we need to inject a size head of the object
  // this is where we do it!
  final override def write(into: OutputStream, e: T): Try[Unit] =
    try {
      unsafeWrite(into, e)
      Serialization.successUnit
    } catch {
      case NonFatal(e) =>
        Failure(e)
    }

  final def read(in: InputStream): Try[T] =
    try {
      Success(unsafeRead(in))
    } catch {
      case NonFatal(e) =>
        Failure(e)
    }

  override def compareBinaryNoConsume(inputStreamA: InputStream, inputStreamB: InputStream): OrderedSerialization.Result =
    try {
      val r = unsafeSwitchingCompareBinaryNoConsume(inputStreamA, inputStreamB, false)
      OrderedSerialization.resultFrom(r)
    } catch {
      case NonFatal(e) =>
        OrderedSerialization.CompareFailure(e)
    }

  override def compareBinary(inputStreamA: InputStream, inputStreamB: InputStream): OrderedSerialization.Result =
    try {
      val r = unsafeSwitchingCompareBinaryNoConsume(inputStreamA, inputStreamB, true)
      OrderedSerialization.resultFrom(r)
    } catch {
      case NonFatal(e) =>
        OrderedSerialization.CompareFailure(e)
    }
}
