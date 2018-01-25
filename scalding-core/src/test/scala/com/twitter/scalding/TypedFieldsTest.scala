/*
Copyright 2012 Twitter, Inc.

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
package com.twitter.scalding

import cascading.flow.FlowException
import org.scalatest.{ Matchers, WordSpec }

class TypedFieldsTest extends WordSpec with Matchers {

  "A fields API job" should {

    // First we check that the untyped version fails because
    // the Opaque class has no comparator

    "throw an exception if a field is not comparable" in {
      val thrown = the[FlowException] thrownBy untypedJob()
      thrown.getMessage shouldBe "local step failed"
    }

    // Now run the typed fields version

    "group by custom comparator correctly" in {
      JobTest(new TypedFieldsJob(_))
        .arg("input", "inputFile")
        .arg("output", "outputFile")
        .source(TextLine("inputFile"), List("0" -> "5,foo", "1" -> "6,bar", "2" -> "9,foo"))
        .sink[(Opaque, Int)](Tsv("outputFile")){ outputBuffer =>
          val outMap = outputBuffer.map { case (opaque: Opaque, i: Int) => (opaque.str, i) }.toMap
          outMap should have size 2
          outMap("foo") shouldBe 14
          outMap("bar") shouldBe 6
        }
        .run
        .finish()

    }

  }

  def untypedJob(): Unit = {
    JobTest(new UntypedFieldsJob(_))
      .arg("input", "inputFile")
      .arg("output", "outputFile")
      .source(TextLine("inputFile"), List("0" -> "5,foo", "1" -> "6,bar", "2" -> "9,foo"))
      .sink[(Opaque, Int)](Tsv("outputFile")){ _ => }
      .run
      .finish()
  }

}

class UntypedFieldsJob(args: Args) extends Job(args) {

  TextLine(args("input")).read
    .map('line -> ('x, 'y)) { line: String =>
      val split = line.split(",")
      (split(0).toInt, new Opaque(split(1)))
    }
    .groupBy('y) { _.sum[Double]('x) }
    .write(Tsv(args("output")))

}

// The only difference here is that we type the Opaque field

class TypedFieldsJob(args: Args) extends Job(args) {

  implicit val ordering: Ordering[Opaque] = new Ordering[Opaque] {
    def compare(a: Opaque, b: Opaque) = a.str compare b.str
  }

  val xField = Field[String]('x)
  val yField = Field[Opaque]('y)

  TextLine(args("input")).read
    .map('line -> (xField, yField)) { line: String =>
      val split = line.split(",")
      (split(0).toInt, new Opaque(split(1)))
    }
    .groupBy(yField) { _.sum[Double](xField -> xField) }
    .write(Tsv(args("output")))

}

// This is specifically not a case class - it doesn't implement any
// useful interfaces (such as Comparable)

class Opaque(val str: String) {
  override def equals(other: Any) = other match {
    case other: Opaque => str equals other.str
    case _ => false
  }
  override def hashCode = str.hashCode
}
