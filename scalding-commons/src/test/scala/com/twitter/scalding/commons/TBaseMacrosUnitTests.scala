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
package com.twitter.scalding.commons

import com.twitter.scalding._
import com.twitter.scalding.commons.thrift.TBaseOrderedSerialization
import com.twitter.scalding.serialization.JavaStreamEnrichments._
import java.io._
import com.twitter.scalding.serialization.OrderedSerialization
import org.apache.thrift.TBase
import org.scalatest.{ Matchers, WordSpec }

class TBaseMacrosUnitTests extends WordSpec with Matchers {
  import TBaseOrderedSerialization._

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

  def rt[T](t: T)(implicit orderedBuffer: OrderedSerialization[T]) = {
    val r = deserializeSeq(40, serializeSeq((0 until 40).map(_ => t)))(orderedBuffer)
    assert(r.distinct.size == 1)
    r.head
  }

  def deserializeSeq[T](items: Int, buf: InputStream)(implicit orderedBuffer: OrderedSerialization[T]): Seq[T] = {
    (0 until items).map { _ =>
      orderedBuffer.read(buf).get
    }.toList
  }

  "TBaseOrderedSerialization" should {

    "Should RT" in {
      val x = new com.twitter.scalding.commons.TestThriftStructure("asdf", 123)
      rt[com.twitter.scalding.commons.TestThriftStructure](x)
    }

    "Should Compare Equal" in {
      val x1 = new com.twitter.scalding.commons.TestThriftStructure("asdf", 123)
      val x2 = new com.twitter.scalding.commons.TestThriftStructure("asdf", 123)
      implicitly[OrderedSerialization[com.twitter.scalding.commons.TestThriftStructure]].compare(x1, x2)
      rawCompare(x1, x2) shouldEqual 0
    }

    "Should Compare LessThan" in {
      val x1 = new com.twitter.scalding.commons.TestThriftStructure("asdf", 122)
      val x2 = new com.twitter.scalding.commons.TestThriftStructure("asdf", 123)
      rawCompare(x1, x2) shouldEqual -1
    }

    "Should Compare GreaterThan" in {
      val x1 = new com.twitter.scalding.commons.TestThriftStructure("asdp", 123)
      val x2 = new com.twitter.scalding.commons.TestThriftStructure("asdf", 123)
      rawCompare(x1, x2) shouldEqual 1
    }

  }
}
