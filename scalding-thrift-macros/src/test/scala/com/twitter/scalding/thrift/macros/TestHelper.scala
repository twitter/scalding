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
package com.twitter.scalding.thrift.macros

import com.twitter.bijection.macros.MacroGenerated
import com.twitter.scalding.serialization.JavaStreamEnrichments._
import com.twitter.scalding.serialization.OrderedSerialization
import java.io._
import org.scalatest.Matchers

object TestHelper extends Matchers {

  def isMg[T](t: T): T = {
    t shouldBe a[MacroGenerated]
    t
  }

  def rt[T](t: T)(implicit orderedBuffer: OrderedSerialization[T]) = {
    val r = deserializeSeq(40, serializeSeq((0 until 40).map(_ => t)))
    assert(r.distinct.size == 1)
    r.head
  }

  def oBufCompare[T](a: T, b: T)(implicit obuf: OrderedSerialization[T]): Int = {
    obuf.compare(a, b)
  }

  def deserializeSeq[T](items: Int, buf: InputStream)(implicit orderedBuffer: OrderedSerialization[T]): Seq[T] = {
    (0 until items).map { _ =>
      orderedBuffer.read(buf).get
    }.toList
  }

  def serialize[T](t: T)(implicit orderedBuffer: OrderedSerialization[T]): InputStream =
    serializeSeq(List(t))

  def serializeSeq[T](t: Seq[T])(implicit orderedBuffer: OrderedSerialization[T]): InputStream = {
    val baos = new ByteArrayOutputStream
    t.foreach({ e =>
      orderedBuffer.write(baos, e)
    })
    baos.toInputStream
  }

  def rawCompare[T](a: T, b: T)(implicit obuf: OrderedSerialization[T]): Int = {
    obuf.compareBinary(serialize(a), serialize(b)).unsafeToInt
  }

  def checkManyExplicit[T](i: List[T])(implicit obuf: OrderedSerialization[T]) = {
    val serializedA = serializeSeq(i)
    val serializedB = serializeSeq(i)
    (0 until i.size).foreach { _ =>
      assert(obuf.compareBinary(serializedA, serializedB).unsafeToInt === 0)
    }
  }

  def compareSerialized[T](a: T, b: T)(implicit orderedBuffer: OrderedSerialization[T]): OrderedSerialization.Result = {
    val bufA = serializeSeq[T]((0 until 20).map(_ => a))
    val bufB = serializeSeq[T]((0 until 20).map(_ => b))
    val r = (0 until 20).map { _ =>
      orderedBuffer.compareBinary(bufA, bufB)
    }
    if (r.distinct.size == 1) r.head
    else sys.error("Results are inconsistent.." + r)
  }

}
