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

import com.twitter.scalding.serialization.JavaStreamEnrichments._
import java.io.InputStream
import java.io.OutputStream

trait HasUnsafeCompareBinary[T] extends OrderedSerialization[T] {
  def unsafeCompareBinary(inputStreamA: InputStream, inputStreamB: InputStream): Int
  def unsafeWrite(out: java.io.OutputStream, t: T): Unit
  def unsafeRead(in: java.io.InputStream): T
  def unsafeSize(t: T): Option[Int]
}

object HasUnsafeCompareBinary {
  def apply[T](ord: OrderedSerialization[T]): HasUnsafeCompareBinary[T] = ord match {
    case e: HasUnsafeCompareBinary[T] => e
    case o =>
      new HasUnsafeCompareBinary[T] {
        def unsafeCompareBinary(inputStreamA: InputStream, inputStreamB: InputStream): Int =
          o.compareBinary(inputStreamA, inputStreamB).unsafeToInt

        def hash(x: T): Int = o.hash(x)

        // Members declared in com.twitter.scalding.serialization.OrderedSerialization
        def compareBinary(a: java.io.InputStream, b: java.io.InputStream): com.twitter.scalding.serialization.OrderedSerialization.Result =
          o.compareBinary(a, b)

        // Members declared in scala.math.Ordering
        def compare(x: T, y: T): Int = o.compare(x, y)

        def dynamicSize(e: T) = o.dynamicSize(e)
        def unsafeSize(t: T): Option[Int] = o.dynamicSize(t)

        // Members declared in com.twitter.scalding.serialization.Serialization
        def read(in: java.io.InputStream): scala.util.Try[T] = o.read(in)
        def staticSize: Option[Int] = o.staticSize
        def unsafeWrite(out: java.io.OutputStream, t: T): Unit = o.write(out, t).get

        def unsafeRead(in: java.io.InputStream): T = o.read(in).get

        def write(out: java.io.OutputStream, t: T): scala.util.Try[Unit] = o.write(out, t)
      }
  }
}
