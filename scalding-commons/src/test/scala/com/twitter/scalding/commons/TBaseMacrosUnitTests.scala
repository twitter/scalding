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
import org.scalacheck.{ Arbitrary, Gen }
import org.scalatest.{ Matchers, WordSpec }
import org.scalatest.prop.PropertyChecks

class TBaseMacrosUnitTests extends WordSpec with Matchers with PropertyChecks {
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

  implicit val arbThrift: Arbitrary[TestThriftStructure] = Arbitrary(
    for {
      //str <- implicitly[Arbitrary[String]].arbitrary
      str <- Gen.identifier // the tests currently fail due to singed byte issues with above
      num <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
    } yield new TestThriftStructure(str, num))

  "TBaseOrderedSerialization" should {

    "Should RT" in {
      forAll(minSuccessful(10)) { t: TestThriftStructure => rt(t) }
    }

    "Should Compare Equal" in {
      forAll(minSuccessful(10)) { t: TestThriftStructure =>
        implicitly[OrderedSerialization[TestThriftStructure]].compare(t, t) shouldEqual 0
        rawCompare(t, t) shouldEqual 0
      }
    }

    /**
     * This test is actually violated because of the fact that at runtime strings
     * and binary blobs are identical, but they are compared differently inside
     * the objects.
     * To fix this issue we need to reflective look inside the fields and see if they
     * are strings are ByteBuffer. This weakness should not be an issue for hadoop
     * which only uses comparison of objects to check equality.
     */
    "Comparison should match" in {
      forAll(minSuccessful(100)) { (t1: TestThriftStructure, t2: TestThriftStructure) =>
        def clamp(i: Int) = if (i < 0) -1 else if (i > 0) 1 else 0
        clamp(implicitly[OrderedSerialization[TestThriftStructure]].compare(t1, t2)) shouldEqual clamp(rawCompare(t1, t2))
      }
    }
  }
}
