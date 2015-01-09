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
package com.twitter.scalding.commons.macros

import com.twitter.scalding._

import org.scalatest.{ Matchers, WordSpec }

import com.twitter.scalding.platform.{ HadoopSharedPlatformTest, HadoopPlatformJobTest }
import com.twitter.chill.thrift.TBaseSerializer
import com.twitter.chill.{ IKryoRegistrar, ReflectingRegistrar, ReflectingDefaultRegistrar, ScalaKryoInstantiator }
import com.twitter.chill.java.IterableRegistrar
import org.apache.thrift.TBase
import com.twitter.chill.config.{ ConfiguredInstantiator, ScalaAnyRefMapConfig }
import com.twitter.scalding.commons.macros.impl.{ ScroogeOrderedBufferableImpl, TBaseOrderedBufferableImpl }
import com.twitter.scalding.commons.thrift.{ ScroogeOrderedBufferable, TBaseOrderedBufferable }
import com.twitter.scalding.typed.OrderedBufferable
import scala.language.experimental.{ macros => sMacros }
import com.twitter.scrooge.ThriftStruct
import com.twitter.scalding.commons.macros.scalathrift._
import org.scalacheck.Arbitrary

class ThriftCompareJob(args: Args) extends Job(args) {
  val tp = TypedPipe.from((0 until 100).map { idx =>
    new TestThriftStructure("asdf", idx % 10)
  })
  tp.map(_ -> 1L).sumByKey.map {
    case (k, v) =>
      (k.toString, v)
  }.write(TypedTsv[(String, Long)]("output"))
}

class CompareJob[T: OrderedBufferable](in: Iterable[T], args: Args) extends Job(args) {
  TypedPipe.from(in).map(_ -> 1L).sumByKey.map {
    case (k, v) =>
      (k.hashCode, v)
  }.write(TypedTsv[(Int, Long)]("output"))
}
private[macros] trait InstanceProvider[T] {
  def g(idx: Int): T
}
class PlatformTest extends WordSpec with Matchers with HadoopSharedPlatformTest {
  org.apache.log4j.Logger.getLogger("org.apache.hadoop").setLevel(org.apache.log4j.Level.FATAL)
  org.apache.log4j.Logger.getLogger("org.mortbay").setLevel(org.apache.log4j.Level.FATAL)
  implicit def toScroogeOrderedBufferable[T <: ThriftStruct]: ScroogeOrderedBufferable[T] = macro ScroogeOrderedBufferableImpl[T]
  implicit def toTBaseOrderedBufferable[T <: TBase[_, _]]: TBaseOrderedBufferable[T] = macro TBaseOrderedBufferableImpl[T]

  import ScroogeGenerators._

  implicit def arbitraryInstanceProvider[T: Arbitrary] = new InstanceProvider[T] {
    def g(idx: Int) = ScroogeGenerators.dataProvider[T](idx)
  }

  implicit def testThriftStructProvider = new InstanceProvider[TestThriftStructure] {
    def g(idx: Int) = new TestThriftStructure("asdf" + idx, idx)
  }

  def runCompareTest[T: OrderedBufferable](implicit iprov: InstanceProvider[T]) {
    val input = (0 until 500).map { idx =>
      iprov.g(idx % 50)
    }

    HadoopPlatformJobTest(new CompareJob[T](input, _), cluster)
      .sink(TypedTsv[(Int, Long)]("output")) { out =>
        import ScroogeGenerators._
        val expected =
          input
            .groupBy(identity)
            .map{ case (k, v) => (k.hashCode, v.size) }

        out.toSet shouldBe expected.toSet
      }
      .run
  }

  "TBase Test" should {
    "Expected items should match: TestThriftStructure" in {
      runCompareTest[TestThriftStructure]
    }
  }

  "ThriftStruct Test" should {

    "Expected items should match : TestStruct" in {
      runCompareTest[TestStruct]
    }

    "Expected items should match : TestSets" in {
      runCompareTest[TestSets]
    }

    "Expected items should match : TestLists" in {
      runCompareTest[TestLists]
    }

    "Expected items should match : TestMaps" in {
      runCompareTest[TestMaps]
    }

    "Expected items should match : TestTypes" in {
      runCompareTest[TestTypes]
    }
  }

}
