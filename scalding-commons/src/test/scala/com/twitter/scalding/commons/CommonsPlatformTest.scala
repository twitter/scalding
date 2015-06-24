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

import org.scalatest.{ Matchers, WordSpec }

import com.twitter.scalding.platform.{ HadoopSharedPlatformTest, HadoopPlatformJobTest }
import org.apache.thrift.TBase
import com.twitter.scalding.commons.thrift.TBaseOrderedSerialization
import com.twitter.scalding.serialization.OrderedSerialization

class CompareJob[T: OrderedSerialization](in: Iterable[T], args: Args) extends Job(args) {
  TypedPipe.from(in).flatMap{ i =>
    (0 until 1).map (_ => i)
  }.map(_ -> 1L).sumByKey.map {
    case (k, v) =>
      (k.hashCode, v)
  }.write(TypedTsv[(Int, Long)]("output"))
}

private[commons] trait InstanceProvider[T] {
  def g(idx: Int): T
}

class CommonsPlatformTest extends WordSpec with Matchers with HadoopSharedPlatformTest {
  org.apache.log4j.Logger.getLogger("org.apache.hadoop").setLevel(org.apache.log4j.Level.FATAL)
  org.apache.log4j.Logger.getLogger("org.mortbay").setLevel(org.apache.log4j.Level.FATAL)

  import TBaseOrderedSerialization._

  implicit def testThriftStructProvider = new InstanceProvider[TestThriftStructure] {
    def g(idx: Int) = new TestThriftStructure("asdf" + idx, idx)
  }

  def runCompareTest[T: OrderedSerialization](implicit iprov: InstanceProvider[T]) {
    val input = (0 until 10000).map { idx =>
      iprov.g(idx % 50)
    }

    HadoopPlatformJobTest(new CompareJob[T](input, _), cluster)
      .sink(TypedTsv[(Int, Long)]("output")) { out =>
        val expected =
          input
            .groupBy(identity)
            .map{ case (k, v) => (k.hashCode, v.size) }

        out.toSet shouldBe expected.toSet
      }
      .run
  }

  "TBase Test" should {
    "Expected items should match: TestThriftStructure2" in {
      runCompareTest[TestThriftStructure]
    }
  }
}
