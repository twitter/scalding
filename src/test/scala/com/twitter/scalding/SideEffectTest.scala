package com.twitter.scalding

import scala.annotation.tailrec
import cascading.pipe._
import org.specs._

/*
 * Zip uses side effect construct to create zipped list.
 */
class Zip(args : Args) extends Job(args) {

  //import RichPipe._
  def createState = new {
    var lastLine: String = null
    def release() {}
  }

  val zipped = Tsv("line",('line)).pipe
    .using { createState }
      .flatMap[String, (String, String)] ('line -> ('l1, 'l2)) { case (accu, line) =>
        if (accu.lastLine == null) {
          accu.lastLine = line
          List()
        } else {
          val zipped = List((accu.lastLine, line))
          accu.lastLine = line
          zipped
        }
      }
    .project('l1, 'l2)

  zipped.write(Tsv("zipped"))
}

class SideEffectTest extends Specification with TupleConversions with FieldConversions {
  "Zipper should do create zipped sequence. Coded with side effect" should {
    JobTest("com.twitter.scalding.Zip")
      .source(Tsv("line",('line)), List(Tuple1("line1"), Tuple1("line2"), Tuple1("line3"), Tuple1("line4")))
      .sink[(String, String)](Tsv("zipped")) { ob =>
        "correctly compute zipped sequence" in {
          val res = ob.toList
          val expected = List(("line1", "line2"), ("line2", "line3"), ("line3", "line4"))
          res.zip(expected) foreach {
            case ((a, b), (c, d)) =>
              a must be_== ( c )
              b must be_== ( d )
          }
        }
      }
      .run
      .finish
  }
}

/*
 * ZipBuffer uses (unneccessary) side effect to construct zipped.
 */
class ZipBuffer(args : Args) extends Job(args) {

  //import RichPipe._
  def createState = new {
    var lastLine: String = null
    def release() {}
  }

  val zipped = Tsv("line",('line)).pipe
    .map('line -> 'oddOrEven) { line : String => line.substring(line.length-1).toInt % 2 match {
      case 0 => "even"
      case 1 => "odd"
    }}
    .groupBy('oddOrEven) {
      _.using { createState }
      .mapStream('line -> ('l1, 'l2)) { (accu, iter : Iterator[String]) => {
        accu.lastLine = iter.next()
        for (line <- iter) yield {
          val result = (accu.lastLine, line)
          accu.lastLine = line
          result
        }
      }}
    }
  .project('l1, 'l2)
                                         
  zipped.write(Tsv("zipped"))
}

class SideEffectBufferTest extends Specification with TupleConversions with FieldConversions {
  "ZipBuffer should do create two zipped sequences, one for even lines and one for odd lines. Coded with side effect" should {
    JobTest("com.twitter.scalding.ZipBuffer")
      .source(Tsv("line",('line)), List(Tuple1("line1"), Tuple1("line2"), Tuple1("line3"), Tuple1("line4"), Tuple1("line5"), Tuple1("line6")))
      .sink[(String, String)](Tsv("zipped")) { ob =>
        "correctly compute zipped sequence" in {
          val res = ob.toList.sorted
          val expected = List(("line1", "line3"), ("line3", "line5"), ("line2", "line4"), ("line4", "line6")).sorted
          res.zip(expected) foreach {
            case ((a, b), (c, d)) =>
              a must be_== ( c )
              b must be_== ( d )
          }
        }
      }
      .run
      .finish
  }
}
