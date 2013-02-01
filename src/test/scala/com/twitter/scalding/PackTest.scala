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

import org.specs._
import scala.reflect.BeanProperty

class IntContainer {
  private var firstValue = 0
  def getFirstValue = firstValue
  def setFirstValue(v : Int) { firstValue = v }

  @BeanProperty // Test the other syntax
  var secondValue = 0
}

object FatContainer {
  def fromFibonacci(first : Int, second : Int) = {
    val fc = new FatContainer
    fc.f1 = first
    fc.f2 = second
    fc.f3 = fc.f1 + fc.f2
    fc.f4 = fc.f2 + fc.f3
    fc.f5 = fc.f3 + fc.f4
    fc.f6 = fc.f4 + fc.f5
    fc.f7 = fc.f5 + fc.f6
    fc.f8 = fc.f6 + fc.f7
    fc.f9 = fc.f7 + fc.f8
    fc.f10 = fc.f8 + fc.f9
    fc.f11 = fc.f9 + fc.f10
    fc.f12 = fc.f10 + fc.f11
    fc.f13 = fc.f11 + fc.f12
    fc.f14 = fc.f12 + fc.f13
    fc.f15 = fc.f13 + fc.f14
    fc.f16 = fc.f14 + fc.f15
    fc.f17 = fc.f15 + fc.f16
    fc.f18 = fc.f16 + fc.f17
    fc.f19 = fc.f17 + fc.f18
    fc.f20 = fc.f18 + fc.f19
    fc.f21 = fc.f19 + fc.f20
    fc.f22 = fc.f20 + fc.f21   
    fc.f23 = fc.f21 + fc.f22
    fc
  }
}

class FatContainer {
  @BeanProperty var f1 = 0
  @BeanProperty var f2 = 0
  @BeanProperty var f3 = 0
  @BeanProperty var f4 = 0
  @BeanProperty var f5 = 0
  @BeanProperty var f6 = 0
  @BeanProperty var f7 = 0
  @BeanProperty var f8 = 0
  @BeanProperty var f9 = 0
  @BeanProperty var f10 = 0
  @BeanProperty var f11 = 0
  @BeanProperty var f12 = 0
  @BeanProperty var f13 = 0
  @BeanProperty var f14 = 0
  @BeanProperty var f15 = 0
  @BeanProperty var f16 = 0
  @BeanProperty var f17 = 0
  @BeanProperty var f18 = 0
  @BeanProperty var f19 = 0
  @BeanProperty var f20 = 0
  @BeanProperty var f21 = 0
  @BeanProperty var f22 = 0
  @BeanProperty var f23 = 0
}

case class IntCaseClass(firstValue : Int, secondValue : Int)

class ContainerPopulationJob (args : Args) extends Job(args) {
  Tsv("input")
    .read
    .mapTo((0, 1) -> ('firstValue, 'secondValue)) { v : (Int, Int) => v}
    .pack[IntContainer](('firstValue, 'secondValue) -> 'combined)
    .project('combined)
    .unpack[IntContainer]('combined -> ('firstValue, 'secondValue))
    .project('firstValue, 'secondValue)
    .write(Tsv("output"))
}

class ContainerToPopulationJob (args : Args) extends Job(args) {
  Tsv("input")
    .read
    .mapTo((0, 1) -> ('firstValue, 'secondValue)) { v : (Int, Int) => v}
    .packTo[IntContainer](('firstValue, 'secondValue) -> 'combined)
    .unpackTo[IntContainer]('combined -> ('firstValue, 'secondValue))
    .write(Tsv("output"))

  Tsv("input")
    .read
    .mapTo((0, 1) -> ('firstValue, 'secondValue)) { v : (Int, Int) => v}
    .packTo[IntCaseClass](('firstValue, 'secondValue) -> 'combined)
    .unpackTo[IntCaseClass]('combined -> ('firstValue, 'secondValue))
    .write(Tsv("output-cc"))
}

class FatContainerPopulationJob (args : Args) extends Job(args) {
  Tsv("input")
    .read
    .mapTo((0, 1) -> ('firstValue, 'secondValue)) { v : (Int, Int) => v}
    .map(('firstValue, 'secondValue) -> 'fatContainer) { v : (Int, Int) =>
      FatContainer.fromFibonacci(v._1, v._2)
    }
    .unpack[FatContainer]('fatContainer -> '*)
    .discard('firstValue, 'secondValue, 'fatContainer)
    .write(JsonLine("output"))
}

class FatContainerToPopulationJob (args : Args) extends Job(args) {
  Tsv("input")
    .read
    .mapTo((0, 1) -> ('firstValue, 'secondValue)) { v : (Int, Int) => v}
    .map(('firstValue, 'secondValue) -> 'fatContainer) { v : (Int, Int) =>
      FatContainer.fromFibonacci(v._1, v._2)
    }
    .unpackTo[FatContainer]('fatContainer -> '*)
    .write(JsonLine("output"))
}

class PackTest extends Specification with TupleConversions {
  noDetailedDiffs()

  val inputData = List(
    (1, 2),
    (2, 2),
    (3, 2)
  )

  "A ContainerPopulationJob" should {
    JobTest("com.twitter.scalding.ContainerPopulationJob")
      .source(Tsv("input"), inputData)
      .sink[(Int, Int)](Tsv("output")) { buf =>
        "correctly populate container objects" in {
          buf.size must_== 3
          buf.toSet must_== inputData.toSet
        }
      }
      .run
      .finish
  }

  "A ContainerToPopulationJob" should {
    JobTest("com.twitter.scalding.ContainerToPopulationJob")
      .source(Tsv("input"), inputData)
      .sink[(Int, Int)](Tsv("output")) { buf =>
        "correctly populate container objects" in {
          buf.size must_== 3
          buf.toSet must_== inputData.toSet
        }
      }
      .sink[(Int, Int)](Tsv("output-cc")) { buf =>
        "correctly populate container case class objects" in {
          buf.size must_== 3
          buf.toSet must_== inputData.toSet
        }
      }
      .run
      .finish
  }

  val fatInputData = List((8, 13))
  val fatCorrect = """{"f20":75025,"f19":46368,"f7":144,"f6":89,"f14":4181,"f1":8,"f13":2584,"f18":28657,"f8":233,"f10":610,"f5":55,"f21":121393,"f3":21,"f9":377,"f17":17711,"f4":34,"f11":987,"f22":196418,"f15":6765,"f16":10946,"f23":317811,"f2":13,"f12":1597}"""

  "A FatContainerPopulationJob" should {
    JobTest("com.twitter.scalding.FatContainerPopulationJob")
      .source(Tsv("input"), fatInputData)
      .sink[String](JsonLine("output")) { buf =>
        "correctly populate a fat container object" in {
          buf.size must_== 1
          buf.head must_== fatCorrect
        }
      }
      .run
      .finish    
  }

  "A FatContainerToPopulationJob" should {
    JobTest("com.twitter.scalding.FatContainerPopulationJob")
      .source(Tsv("input"), fatInputData)
      .sink[String](JsonLine("output")) { buf =>
        "correctly populate a fat container object" in {
          buf.size must_== 1
          buf.head must_== fatCorrect
        }
      }
      .run
      .finish    
  }
}
