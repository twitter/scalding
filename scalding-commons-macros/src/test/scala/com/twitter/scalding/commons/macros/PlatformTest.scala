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

import com.twitter.scalding.platform.{ HadoopPlatformTest, HadoopPlatformJobTest }
import com.twitter.chill.thrift.TBaseSerializer
import com.twitter.chill.{ IKryoRegistrar, ReflectingRegistrar, ReflectingDefaultRegistrar, ScalaKryoInstantiator }
import com.twitter.chill.java.IterableRegistrar
import org.apache.thrift.TBase
import com.twitter.chill.config.{ ConfiguredInstantiator, ScalaAnyRefMapConfig }
import com.twitter.scalding.commons.macros.impl.TBaseOrderedBufferableImpl
import com.twitter.scalding.commons.thrift.TBaseOrderedBufferable
import scala.language.experimental.{ macros => sMacros }

object ThriftCompareJob {
  val inputData = (0 until 100).map{ idx =>
    new TestThriftStructure("asdf", idx % 10)
  }
  val expectedResults = inputData.map(_.toString).groupBy(identity).map{ case (k, v) => (k, v.size) }
}

class ThriftCompareJob(args: Args) extends Job(args) {
  implicit def toTBaseOrderedBufferable[T <: TBase[_, _]]: TBaseOrderedBufferable[T] = macro TBaseOrderedBufferableImpl[T]

  override def ioSerializations =
    List(
      classOf[com.twitter.scalding.serialization.WrappedSerialization[Object]]) ++ super.ioSerializations

  val tp = TypedPipe.from((0 until 100)).forceToDisk.map { idx =>
    new TestThriftStructure("asdf", idx % 10)
  }
  tp.map(_ -> 1L).sumByKey.map {
    case (k, v) =>
      (k.toString, v)
  }.write(TypedTsv[(String, Long)]("output"))
}

class PlatformTest extends WordSpec with Matchers with HadoopPlatformTest {
  org.apache.log4j.Logger.getLogger("org.apache.hadoop").setLevel(org.apache.log4j.Level.FATAL)
  org.apache.log4j.Logger.getLogger("org.mortbay").setLevel(org.apache.log4j.Level.FATAL)

  "An InAndOutTest" should {
    "Should be able to count number of thrift items" in {
      HadoopPlatformJobTest(new ThriftCompareJob(_), cluster)
        .sink(TypedTsv[(String, Long)]("output")) { _.toSet shouldBe (ThriftCompareJob.expectedResults.toSet) }
        .run
    }
  }

}
