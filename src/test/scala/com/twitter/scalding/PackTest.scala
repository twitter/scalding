package com.twitter.scalding

import org.specs._

class TuplePopulationJob (args : Args) extends Job(args) {
  Tsv("input")
    .read
    .mapTo((0, 1, 2) -> ('f1, 'f2, 'f3)) { v : (Int, Int, String) => v}
    .pack[(Int, Int, String)](('f1, 'f2, 'f3) -> 'combined)
    .unpack[(Int, Int, String)]('combined -> ('f4, 'f5, 'f6))
    .project('f4, 'f5, 'f6)
    .write(Tsv("output"))
}

class IntContainer {
  private var firstValue = 0
  private var secondValue = 0
  def getFirstValue = firstValue
  def getSecondValue = secondValue
  def setFirstValue(v : Int) { firstValue = v }
  def setSecondValue(v : Int) { secondValue = v }
}

class ContainerPopulationJob (args : Args) extends Job(args) {
  implicit def containerPacker[IntContainer](implicit m : Manifest[IntContainer]) = new ReflectionTuplePacker[IntContainer]()(m)
  implicit def containerUnpacker[IntContainer](implicit m : Manifest[IntContainer]) = new ReflectionTupleUnpacker[IntContainer]()(m)
  Tsv("input")
    .read
    .mapTo((0, 1) -> ('firstValue, 'secondValue)) { v : (Int, Int) => v}
    .pack[IntContainer](('firstValue, 'secondValue) -> 'combined)
    .project('combined)
    .unpack[IntContainer]('combined -> ('firstValue, 'secondValue))
    .project('firstValue, 'secondValue)
    .write(Tsv("output"))
}

class PackTest extends Specification with TupleConversions {
  noDetailedDiffs()
  val inputData = List(
    (1, 2, "a"),
    (2, 2, "b"),
    (3, 2, "c")
  )

  val inputData2 = List(
    (1, 2),
    (2, 2),
    (3, 2)
  )

  "A TuplePopulationJob" should {
    JobTest("com.twitter.scalding.TuplePopulationJob")
      .source(Tsv("input"), inputData)
      .sink[(Int, Int, String)](Tsv("output")) { buf =>
        "correctly populate tuples" in {
          buf.size must_== 3
          buf.toSet must_== inputData.toSet
        }
      }
      .run
      .finish
  }

  "A ContainerPopulationJob" should {
    JobTest("com.twitter.scalding.ContainerPopulationJob")
      .source(Tsv("input"), inputData2)
      .sink[(Int, Int)](Tsv("output")) { buf =>
        "correctly populate container objects" in {
          buf.size must_== 3
          buf.toSet must_== inputData2.toSet
        }
      }
      .run
      .finish
  }
}
