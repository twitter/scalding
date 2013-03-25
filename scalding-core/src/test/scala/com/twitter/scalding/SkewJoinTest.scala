package com.twitter.scalding

import org.specs._

import cascading.pipe.joiner._

import java.lang.reflect.InvocationTargetException

import scala.collection.mutable.Buffer

class SkewJoinJob(args : Args) extends Job(args) {
  val sampleRate = args.getOrElse("sampleRate", "0.001").toDouble
  val reducers = args.getOrElse("reducers", "-1").toInt
  val replicationFactor = args.getOrElse("replicationFactor", "1").toInt
  val replicator = if (args.getOrElse("replicator", "a") == "a")
                     SkewReplicationA(replicationFactor)
                   else
                     SkewReplicationB()

  val in0 = Tsv("input0").read.mapTo((0,1,2) -> ('x1, 'y1, 's1)) { input : (Int, Int, Int) => input }
  val in1 = Tsv("input1").read.mapTo((0,1,2) -> ('x2, 'y2, 's2)) { input : (Int, Int, Int) => input }

  in0
    .skewJoinWithSmaller('y1 -> 'y2, in1, sampleRate, reducers, replicator)
    .project('x1, 'y1, 's1, 'x2, 'y2, 's2)
    .write(Tsv("output"))
  // Normal inner join:
   in0
    .joinWithSmaller('y1 -> 'y2, in1)
    .project('x1, 'y1, 's1, 'x2, 'y2, 's2)
    .write(Tsv("jws-output"))
}

class SkewJoinPipeTest extends Specification with TupleConversions {
  noDetailedDiffs()

  "A SkewInnerProductJob" should {
    val rng = new java.util.Random
    def generateInput(size: Int, max: Int): List[(String,String,String)] = {
      def next: String = rng.nextInt(max).toString
      (0 to size).map { i => (next, next, next) }.toList
    }

    type JoinResult = (Int,Int,Int,Int,Int,Int)

    def runJobWithArguments(sampleRate : Double = 0.001, reducers : Int = -1,
                            replicationFactor : Int = 1, replicator : String = "a"): (Set[JoinResult], Set[JoinResult]) = {

      var skewResult = Buffer[JoinResult]()
      var innerResult = Buffer[JoinResult]()
      JobTest(new SkewJoinJob(_))
        .source(Tsv("input0"), generateInput(10000, 100))
        .source(Tsv("input1"), generateInput(10000, 100))
        .sink[(Int,Int,Int,Int,Int,Int)](Tsv("output")) { outBuf => skewResult ++ outBuf }
        .sink[(Int,Int,Int,Int,Int,Int)](Tsv("jws-output")) { outBuf => innerResult ++ outBuf }
        .run
        //.runHadoop //this takes MUCH longer to run. Commented out by default, but tests pass on my machine
        .finish
      (skewResult.toSet, innerResult.toSet)
    }

    "compute skew join with sampleRate = 0.001, using strategy A" in {
      runJobWithArguments(sampleRate = 0.001, replicator = "a") match { case (sk: Set[JoinResult], inner: Set[JoinResult]) =>
        sk must_== inner
      }
    }

    "compute skew join with sampleRate = 0.001, using strategy B" in {
      runJobWithArguments(sampleRate = 0.001, replicator = "b") match { case (sk: Set[JoinResult], inner: Set[JoinResult]) =>
        sk must_== inner
      }
    }

    "compute skew join with sampleRate = 0.9, using strategy A" in {
      runJobWithArguments(sampleRate = 0.9, replicator = "a") match { case (sk: Set[JoinResult], inner: Set[JoinResult]) =>
        sk must_== inner
      }
    }

    "compute skew join with sampleRate = 0.9, using strategy B" in {
      runJobWithArguments(sampleRate = 0.9, replicator = "b") match { case (sk: Set[JoinResult], inner: Set[JoinResult]) =>
        sk must_== inner
      }
    }

    "compute skew join with replication factor 5, using strategy A" in {
      runJobWithArguments(replicationFactor = 5, replicator = "a") match { case (sk: Set[JoinResult], inner: Set[JoinResult]) =>
        sk must_== inner
      }
    }

    "compute skew join with reducers = 10, using strategy A" in {
      runJobWithArguments(reducers = 10, replicator = "a") match { case (sk: Set[JoinResult], inner: Set[JoinResult]) =>
        sk must_== inner
      }
    }

    "compute skew join with reducers = 10, using strategy B" in {
      runJobWithArguments(reducers = 10, replicator = "b") match { case (sk: Set[JoinResult], inner: Set[JoinResult]) =>
        sk must_== inner
      }
    }
  }
}

class CollidingKeySkewJoinJob(args : Args) extends Job(args) {
  val sampleRate = args.getOrElse("sampleRate", "0.001").toDouble
  val reducers = args.getOrElse("reducers", "-1").toInt
  val replicationFactor = args.getOrElse("replicationFactor", "1").toInt
  val replicator = if (args.getOrElse("replicator", "a") == "a")
                     SkewReplicationA(replicationFactor)
                   else
                     SkewReplicationB()

  val in0 = Tsv("input0").read.mapTo((0,1,2) -> ('k1, 'k3, 'v1)) { input : (Int, Int, Int) => input }
  val in1 = Tsv("input1").read.mapTo((0,1,2) -> ('k2, 'k3, 'v2)) { input : (Int, Int, Int) => input }

  in0
    .skewJoinWithSmaller(('k1, 'k3) -> ('k2, 'k3), in1, sampleRate, reducers, replicator)
    .project('k1, 'k3, 'v1, 'k2, 'v2)
    .write(Tsv("output"))
}

class CollidingKeySkewJoinTest extends Specification with TupleConversions {
  noDetailedDiffs()

  "A SkewInnerProductJob" should {

    val in1 = List(("0", "0", "1"), ("0", "1", "1"), ("1", "0", "2"), ("2", "0", "4"))
    val in2 = List(("0", "1", "1"), ("1", "0", "2"), ("2", "4", "5"))
    val correctOutput = Set((0, 0, 1, 1, 0), (1, 0, 2, 1, 0), (2, 0, 4, 1, 0), (0, 1, 1, 0, 1))

    def runJobWithArguments(sampleRate : Double = 0.001, reducers : Int = -1,
                            replicationFactor : Int = 1, replicator : String = "a")
                            (callback : Buffer[(Int,Int,Int,Int,Int)] => Unit ) {

      JobTest("com.twitter.scalding.SkewJoinJob")
        .source(Tsv("input0"), in1)
        .source(Tsv("input1"), in2)
        .sink[(Int,Int,Int,Int,Int)](Tsv("output")) { outBuf =>
          callback(outBuf)
        }
        .run
        .finish
    }

    "compute skew join with colliding fields, using strategy A" in {
      runJobWithArguments(replicator = "a") { outBuf =>
        val unordered = outBuf.toSet
        unordered must_== correctOutput
      }
    }

    "compute skew join with colliding fields, using strategy B" in {
      runJobWithArguments(replicator = "b") { outBuf =>
        val unordered = outBuf.toSet
        unordered must_== correctOutput
      }
    }
  }
}
