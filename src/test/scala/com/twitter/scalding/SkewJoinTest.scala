package com.twitter.scalding

import org.specs._

import cascading.pipe.joiner._

import java.lang.reflect.InvocationTargetException

import scala.collection.mutable.Buffer

class SkewInnerProductJob(args : Args) extends Job(args) {
  val sampleRate = args.getOrElse("sampleRate", "0.001").toDouble
  val replicationFactor = args.getOrElse("replicationFactor", "1").toInt

  val in0 = Tsv("input0").read.mapTo((0,1,2) -> ('x1, 'y1, 's1)) { input : (Int, Int, Int) => input }
  val in1 = Tsv("input1").read.mapTo((0,1,2) -> ('x2, 'y2, 's2)) { input : (Int, Int, Int) => input }

  in0
    .skewJoinWithSmaller('y1 -> 'y2, in1)
    .project('x1, 'y1, 's1, 'x2, 'y2, 's2)
    .write(Tsv("output"))
}

class SkewJoinPipeTest extends Specification with TupleConversions {
  noDetailedDiffs()

  "A SkewInnerProductJob" should {

    val in1 = List(("0", "0", "1"), ("0", "1", "1"), ("1", "0", "2"), ("2", "0", "4"))
    val in2 = List(("0", "1", "1"), ("1", "0", "2"), ("2", "4", "5"))
    val correctOutput = Set((0, 0, 1, 1, 0, 2), (0, 1, 1, 0, 1, 1), (1, 0, 2, 1, 0, 2), (2, 0, 4, 1, 0, 2))

    def runJobWithArguments(sampleRate : Double = 0.001, replicationFactor : Int = 1)
        (callback : Buffer[(Int,Int,Int,Int,Int,Int)] => Unit ) {
      JobTest("com.twitter.scalding.SkewInnerProductJob")
        .source(Tsv("input0"), in1)
        .source(Tsv("input1"), in2)
        .sink[(Int,Int,Int,Int,Int,Int)](Tsv("output")) { outBuf =>
          callback(outBuf)
        }
        .run
        .finish
    }

    "correctly compute product with sampleRate = 0.001 and replicationFactor = 1" in {
      runJobWithArguments() { outBuf =>
        val unordered = outBuf.toSet
        unordered must_== correctOutput
      }
    }

    "correctly compute product with sampleRate = 0.99" in {
      runJobWithArguments(sampleRate = 0.99) { outBuf =>
        val unordered = outBuf.toSet
        unordered must_== correctOutput
      }
    }

    "correctly compute product with replicationFactor = 5" in {
      runJobWithArguments(replicationFactor = 5) { outBuf =>
        val unordered = outBuf.toSet
        unordered must_== correctOutput
      }
    }
  }
}
