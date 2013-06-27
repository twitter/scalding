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

import cascading.tuple.Fields
import cascading.tuple.TupleEntry
import java.util.concurrent.TimeUnit

import org.specs._
import java.lang.{Integer => JInt}

class NumberJoinerJob(args : Args) extends Job(args) {
  val in0 = TypedTsv[(Int,Int)]("input0").read.rename((0,1) -> ('x0, 'y0))
  val in1 = Tsv("input1").read.mapTo((0,1) -> ('x1, 'y1)) { input : (Long, Long) => input }
  in0.joinWithSmaller('x0 -> 'x1, in1)
  .write(Tsv("output"))
}

class NumberJoinTest extends Specification {
  import Dsl._
  "A NumberJoinerJob" should {
    //Set up the job:
    "not throw when joining longs with ints" in {
      JobTest("com.twitter.scalding.NumberJoinerJob")
        .source(TypedTsv[(Int,Int)]("input0"), List((0,1), (1,2), (2,4)))
        .source(Tsv("input1"), List(("0","1"), ("1","3"), ("2","9")))
        .sink[(Int,Int,Long,Long)](Tsv("output")) { outBuf =>
          val unordered = outBuf.toSet
          unordered.size must be_==(3)
          unordered((0,1,0L,1L)) must be_==(true)
          unordered((1,2,1L,3L)) must be_==(true)
          unordered((2,4,2L,9L)) must be_==(true)
        }
        .run
        .runHadoop
        .finish
    }
  }
}

object GroupRandomlyJob {
  val NumShards = 10
}

class GroupRandomlyJob(args: Args) extends Job(args) {
  import GroupRandomlyJob.NumShards

  Tsv("fakeInput").read
    .mapTo(0 -> 'num) { (line: String) => line.toInt }
    .groupRandomly(NumShards) { _.max('num) }
    .groupAll { _.size }
    .write(Tsv("fakeOutput"))
}

class GroupRandomlyJobTest extends Specification {
  import GroupRandomlyJob.NumShards
  noDetailedDiffs()

  "A GroupRandomlyJob" should {
    val input = (0 to 10000).map { _.toString }.map { Tuple1(_) }
    JobTest("com.twitter.scalding.GroupRandomlyJob")
      .source(Tsv("fakeInput"), input)
      .sink[(Int)](Tsv("fakeOutput")) { outBuf =>
        val numShards = outBuf(0)
        numShards must be_==(NumShards)
      }
      .run.finish
  }
}

class ShuffleJob(args: Args) extends Job(args) {
  Tsv("fakeInput")
    .read
    .mapTo(0 -> 'num) { (line: String) => line.toInt }
    .shuffle(shards = 1, seed = 42L)
    .groupAll{ _.toList[Int]('num -> 'num) }
    .write(Tsv("fakeOutput"))
}

class ShuffleJobTest extends Specification {
  noDetailedDiffs()

  val expectedShuffle : List[Int] = List(10, 5, 9, 12, 0, 1, 4, 8, 11, 6, 2, 3, 7)

  "A ShuffleJob" should {
    val input = (0 to 12).map { Tuple1(_) }
    JobTest("com.twitter.scalding.ShuffleJob")
      .source(Tsv("fakeInput"), input)
      .sink[(List[Int])](Tsv("fakeOutput")) { outBuf =>
        outBuf(0) must be_==(expectedShuffle)
      }
      .run.finish
  }
}

class MapToGroupBySizeSumMaxJob(args: Args) extends Job(args) {
  TextLine(args("input")).read.
  //1 is the line
  mapTo(1-> ('kx,'x)) { line : String =>
    val x = line.toDouble
    ((x > 0.5),x)
  }.
  groupBy('kx) { _.size.sum[Double]('x->'sx).max('x) }.
  write( Tsv(args("output")) )
}

class MapToGroupBySizeSumMaxTest extends Specification {
  noDetailedDiffs()
  "A MapToGroupBySizeSumMaxJob" should {
    val r = new java.util.Random
    //Here is our input data:
    val input = (0 to 100).map { i : Int => (i.toString, r.nextDouble.toString) }
    //Here is our expected output:
    val goldenOutput = input.map { case (line : String, x : String) =>
      val xv = x.toDouble;
      ((xv > 0.5), xv)
      }.
      groupBy { case (kx : Boolean, x : Double) => kx }.
      mapValues { vals =>
        val vlist = vals.map { case (k:Boolean, x:Double) => x }.toList
        val size = vlist.size
        val sum = vlist.sum
        val max = vlist.max
        (size, sum, max)
      }
    //Now we have the expected input and output:
    JobTest("com.twitter.scalding.MapToGroupBySizeSumMaxJob").
      arg("input","fakeInput").
      arg("output","fakeOutput").
      source(TextLine("fakeInput"), input).
      sink[(Boolean,Int,Double,Double)](Tsv("fakeOutput")) { outBuf =>
        val actualOutput = outBuf.map {
          case (k:Boolean, sz : Int, sm : Double, mx : Double) =>
          (k, (sz,sm,mx) )
        }.toMap
        "produce correct size, sum, max" in {
          goldenOutput must be_==(actualOutput)
        }
      }.
      run.
      finish
  }
}

class PartitionJob(args: Args) extends Job(args) {
  Tsv("input", new Fields("age", "weight"))
    .partition('age -> 'isAdult) { (_:Int) > 18 } { _.average('weight) }
    .project('isAdult, 'weight)
    .write(Tsv("output"))
}

class PartitionJobTest extends Specification {
  noDetailedDiffs()
  "A PartitionJob" should {
    val input = List((3, 23),(23,154),(15,123),(53,143),(7,85),(19,195),
      (42,187),(35,165),(68,121),(13,103),(17,173),(2,13))

    val (adults, minors) = input.partition { case (age, _) => age > 18 }
    val Seq(adultWeights, minorWeights) = Seq(adults, minors).map { list =>
      list.map { case (_, weight) => weight }
    }
    val expectedOutput = Map(
      true -> adultWeights.sum / adultWeights.size.toDouble,
      false -> minorWeights.sum / minorWeights.size.toDouble
    )
    JobTest(new com.twitter.scalding.PartitionJob(_))
      .source(Tsv("input", new Fields("age", "weight")), input)
      .sink[(Boolean,Double)](Tsv("output")) { outBuf =>
        outBuf.toMap must be_==(expectedOutput)
      }
      .run.finish
  }
}

class MRMJob(args : Args) extends Job(args) {
  val in = Tsv("input").read.mapTo((0,1) -> ('x,'y)) { xy : (Int,Int) => xy }
   // XOR reduction (insane, I guess:
  in.groupBy('x) { _.reduce('y) { (left : Int, right : Int) => left ^ right } }
    .write(Tsv("outputXor"))
   // set
  val setPipe = in.groupBy('x) { _.mapReduceMap('y -> 'y) { (input : Int) => Set(input) }
    { (left : Set[Int], right : Set[Int]) => left ++ right }
    { (output : Set[Int]) => output.toList }
  }

  setPipe.flatten[Int]('y -> 'y)
  .write(Tsv("outputSet"))

  setPipe.flattenTo[Int]('y -> 'y)
  .write(Tsv("outputSetTo"))
}

class MRMTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  "A MRMJob" should {
    val input = List((0,1),(0,2),(1,3),(1,1))

    JobTest("com.twitter.scalding.MRMJob")
      .source(Tsv("input"), input)
      .sink[(Int,Int)](Tsv("outputXor")) { outBuf =>
        "use reduce to compute xor" in {
          outBuf.toList.sorted must be_==(List((0,3),(1,2)))
        }
      }
      .sink[(Int,Int)](Tsv("outputSet")) { outBuf =>
        "use mapReduceMap to round-trip input" in {
          outBuf.toList.sorted must be_==(input.sorted)
        }
      }
      .sink[Int](Tsv("outputSetTo")) { outBuf =>
        "use flattenTo" in {
          outBuf.toList.sorted must be_==(input.map { _._2 }.sorted)
        }
      }
      .run
      .finish
  }
}

class JoinJob(args: Args) extends Job(args) {
  val p1 = Tsv(args("input1"))
    .read
    .mapTo((0, 1) -> ('k1, 'v1)) { v : (String, Int) => v }
  val p2 = Tsv(args("input2"))
    .read
    .mapTo((0, 1) -> ('k2, 'v2)) { v : (String, Int) => v }
  p1.joinWithSmaller('k1 -> 'k2, p2)
    .project('k1, 'v1, 'v2)
    .write( Tsv(args("output")) )
}

class JoinTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  "A JoinJob" should {
    val input1 = List("a" -> 1, "b" -> 2, "c" -> 3)
    val input2 = List("b" -> -1, "c" -> 5, "d" -> 4)
    val correctOutput = Map("b" -> (2, -1), "c" -> (3, 5))

    JobTest("com.twitter.scalding.JoinJob")
      .arg("input1", "fakeInput1")
      .arg("input2", "fakeInput2")
      .arg("output", "fakeOutput")
      .source(Tsv("fakeInput1"), input1)
      .source(Tsv("fakeInput2"), input2)
      .sink[(String,Int,Int)](Tsv("fakeOutput")) { outBuf =>
        val actualOutput = outBuf.map {
          case (k : String, v1 : Int, v2 : Int) =>
          (k,(v1, v2))
        }.toMap
        "join tuples with the same key" in {
          correctOutput must be_==(actualOutput)
        }
      }
      .run
      .finish
  }
}

class CollidingKeyJoinJob(args: Args) extends Job(args) {
  val p1 = Tsv(args("input1"))
    .read
    .mapTo((0, 1) -> ('k1, 'v1)) { v : (String, Int) => v }
    // An an extra fake key to do a join
    .map('k1 -> 'k2) { (k : String) => k + k }
  val p2 = Tsv(args("input2"))
    .read
    .mapTo((0, 1) -> ('k1, 'v2)) { v : (String, Int) => v }
    // An an extra fake key to do a join
    .map('k1 -> 'k3) { (k : String) => k + k }
  p1.joinWithSmaller(('k1,'k2) -> ('k1,'k3), p2)
    .write( Tsv(args("output")) )
}

class CollidingKeyJoinTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  "A CollidingKeyJoinJob" should {
    val input1 = List("a" -> 1, "b" -> 2, "c" -> 3)
    val input2 = List("b" -> -1, "c" -> 5, "d" -> 4)
    val correctOutput = Map("b" -> (2, "bb", -1, "bb"), "c" -> (3, "cc", 5, "cc"))

    JobTest("com.twitter.scalding.CollidingKeyJoinJob")
      .arg("input1", "fakeInput1")
      .arg("input2", "fakeInput2")
      .arg("output", "fakeOutput")
      .source(Tsv("fakeInput1"), input1)
      .source(Tsv("fakeInput2"), input2)
      .sink[(String,Int,String,Int,String)](Tsv("fakeOutput")) { outBuf =>
        val actualOutput = outBuf.map {
          case (k : String, v1 : Int, k2 : String, v2 : Int, k3 : String) =>
          (k,(v1, k2, v2, k3))
        }.toMap
        "join tuples with the same key" in {
          correctOutput must be_==(actualOutput)
        }
      }
      .run
      .finish
  }
}

class TinyJoinJob(args: Args) extends Job(args) {
  val p1 = Tsv(args("input1"))
    .read
    .mapTo((0, 1) -> ('k1, 'v1)) { v : (String, Int) => v }
  val p2 = Tsv(args("input2"))
    .read
    .mapTo((0, 1) -> ('k2, 'v2)) { v : (String, Int) => v }
  p1.joinWithTiny('k1 -> 'k2, p2)
    .project('k1, 'v1, 'v2)
    .write( Tsv(args("output")) )
}

class TinyJoinTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  "A TinyJoinJob" should {
    val input1 = List("a" -> 1, "b" -> 2, "c" -> 3)
    val input2 = List("b" -> -1, "c" -> 5, "d" -> 4)
    val correctOutput = Map("b" -> (2, -1), "c" -> (3, 5))

    JobTest("com.twitter.scalding.TinyJoinJob")
      .arg("input1", "fakeInput1")
      .arg("input2", "fakeInput2")
      .arg("output", "fakeOutput")
      .source(Tsv("fakeInput1"), input1)
      .source(Tsv("fakeInput2"), input2)
      .sink[(String,Int,Int)](Tsv("fakeOutput")) { outBuf =>
        val actualOutput = outBuf.map {
          case (k : String, v1 : Int, v2 : Int) =>
          (k,(v1, v2))
        }.toMap
        "join tuples with the same key" in {
          correctOutput must be_==(actualOutput)
        }
      }
      .run
      .runHadoop
      .finish
  }
}

class TinyCollisionJoinJob(args: Args) extends Job(args) {
  val p1 = Tsv(args("input1"))
    .read
    .mapTo((0, 1) -> ('k1, 'v1)) { v : (String, Int) => v }
  val p2 = Tsv(args("input2"))
    .read
    .mapTo((0, 1) -> ('k1, 'v2)) { v : (String, Int) => v }
  p1.joinWithTiny('k1 -> 'k1, p2)
    .write( Tsv(args("output")) )
}

class TinyCollisionJoinTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  "A TinyCollisionJoinJob" should {
    val input1 = List("a" -> 1, "b" -> 2, "c" -> 3)
    val input2 = List("b" -> -1, "c" -> 5, "d" -> 4)
    val correctOutput = Map("b" -> (2, -1), "c" -> (3, 5))

    JobTest("com.twitter.scalding.TinyCollisionJoinJob")
      .arg("input1", "fakeInput1")
      .arg("input2", "fakeInput2")
      .arg("output", "fakeOutput")
      .source(Tsv("fakeInput1"), input1)
      .source(Tsv("fakeInput2"), input2)
      .sink[(String,Int,Int)](Tsv("fakeOutput")) { outBuf =>
        val actualOutput = outBuf.map {
          case (k : String, v1 : Int, v2 : Int) =>
          (k,(v1, v2))
        }.toMap
        "join tuples with the same key" in {
          correctOutput must be_==(actualOutput)
        }
      }
      .run
      .finish
  }
}

class TinyThenSmallJoin(args : Args) extends Job(args) {
  val pipe0 = Tsv("in0",('x0,'y0)).read
  val pipe1 = Tsv("in1",('x1,'y1)).read
  val pipe2 = Tsv("in2",('x2,'y2)).read

  pipe0.joinWithTiny('x0 -> 'x1, pipe1)
    .joinWithSmaller('x0 -> 'x2, pipe2)
    .map(('y0, 'y1, 'y2) -> ('y0, 'y1, 'y2)) { v : (TC,TC,TC) =>
      (v._1.n, v._2.n, v._3.n)
    }
    .project('x0, 'y0, 'x1, 'y1, 'x2, 'y2)
    .write(Tsv("out"))
}

case class TC(val n : Int)

class TinyThenSmallJoinTest extends Specification with FieldConversions {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  "A TinyThenSmallJoin" should {
    val input0 = List((1,TC(2)),(2,TC(3)),(3,TC(4)))
    val input1 = List((1,TC(20)),(2,TC(30)),(3,TC(40)))
    val input2 = List((1,TC(200)),(2,TC(300)),(3,TC(400)))
    val correct = List((1,2,1,20,1,200),
      (2,3,2,30,2,300),(3,4,3,40,3,400))

    JobTest("com.twitter.scalding.TinyThenSmallJoin")
      .source(Tsv("in0",('x0,'y0)), input0)
      .source(Tsv("in1",('x1,'y1)), input1)
      .source(Tsv("in2",('x2,'y2)), input2)
      .sink[(Int,Int,Int,Int,Int,Int)](Tsv("out")) { outBuf =>
        val actualOutput = outBuf.toList.sorted
        println(actualOutput)
        "join tuples with the same key" in {
          correct must be_==(actualOutput)
        }
      }
      .run
      .runHadoop
      .finish
  }
}

class LeftJoinJob(args: Args) extends Job(args) {
  val p1 = Tsv(args("input1"))
    .mapTo((0, 1) -> ('k1, 'v1)) { v : (String, Int) => v }
  val p2 = Tsv(args("input2"))
    .mapTo((0, 1) -> ('k2, 'v2)) { v : (String, Int) => v }
  p1.leftJoinWithSmaller('k1 -> 'k2, p2)
    .project('k1, 'v1, 'v2)
    // Null sent to TSV will not be read in properly
    .map('v2 -> 'v2) { v : AnyRef => Option(v).map { _.toString }.getOrElse("NULL") }
    .write( Tsv(args("output")) )
}

class LeftJoinTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  "A LeftJoinJob" should {
    val input1 = List("a" -> 1, "b" -> 2, "c" -> 3)
    val input2 = List("b" -> -1, "c" -> 5, "d" -> 4)
    val correctOutput = Map[String,(Int,AnyRef)]("a" -> (1,"NULL"), "b" -> (2, "-1"),
      "c" -> (3, "5"))

    JobTest("com.twitter.scalding.LeftJoinJob")
      .arg("input1", "fakeInput1")
      .arg("input2", "fakeInput2")
      .arg("output", "fakeOutput")
      .source(Tsv("fakeInput1"), input1)
      .source(Tsv("fakeInput2"), input2)
      .sink[(String,Int,JInt)](Tsv("fakeOutput")) { outBuf =>
        val actualOutput = outBuf.map { input : (String,Int,AnyRef) =>
          println(input)
          val (k, v1, v2) = input
          (k,(v1, v2))
        }.toMap
        "join tuples with the same key" in {
          correctOutput must be_==(actualOutput)
        }
      }
      .run
      .runHadoop
      .finish
  }
}

class LeftJoinWithLargerJob(args: Args) extends Job(args) {
  val p1 = Tsv(args("input1"))
    .mapTo((0, 1) -> ('k1, 'v1)) { v : (String, Int) => v }
  val p2 = Tsv(args("input2"))
    .mapTo((0, 1) -> ('k2, 'v2)) { v : (String, Int) => v }
  // Note i am specifying the joiner explicitly since this did not work properly before (leftJoinWithLarger always worked)
  p1.joinWithLarger('k1 -> 'k2, p2, new cascading.pipe.joiner.LeftJoin)
    .project('k1, 'v1, 'v2)
    // Null sent to TSV will not be read in properly
    .map('v2 -> 'v2) { v : AnyRef => Option(v).map { _.toString }.getOrElse("NULL") }
    .write( Tsv(args("output")) )
}

class LeftJoinWithLargerTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  "A LeftJoinWithLargerJob" should {
    val input1 = List("a" -> 1, "b" -> 2, "c" -> 3)
    val input2 = List("b" -> -1, "c" -> 5, "d" -> 4)
    val correctOutput = Map[String,(Int,AnyRef)]("a" -> (1,"NULL"), "b" -> (2, "-1"),
      "c" -> (3, "5"))

    JobTest("com.twitter.scalding.LeftJoinWithLargerJob")
      .arg("input1", "fakeInput1")
      .arg("input2", "fakeInput2")
      .arg("output", "fakeOutput")
      .source(Tsv("fakeInput1"), input1)
      .source(Tsv("fakeInput2"), input2)
      .sink[(String,Int,JInt)](Tsv("fakeOutput")) { outBuf =>
        val actualOutput = outBuf.map { input : (String,Int,AnyRef) =>
          println(input)
          val (k, v1, v2) = input
          (k,(v1, v2))
        }.toMap
        "join tuples with the same key" in {
          correctOutput must be_==(actualOutput)
        }
      }
      .run
      .runHadoop
      .finish
  }
}

class MergeTestJob(args : Args) extends Job(args) {
  val in = TextLine(args("in")).read.mapTo(1->('x,'y)) { line : String =>
    val p = line.split(" ").map { _.toDouble }
    (p(0),p(1))
  }
  val big = in.filter('x) { (x:Double) => (x > 0.5) }
  val small = in.filter('x) { (x:Double) => (x <= 0.5) }
  (big ++ small).groupBy('x) { _.max('y) }
  .write(Tsv(args("out")))
}

class MergeTest extends Specification {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  "A MergeTest" should {
    val r = new java.util.Random
    //Here is our input data:
    val input = (0 to 100).map { i => (i.toString, r.nextDouble.toString +" "+ r.nextDouble.toString) }
    //Here is our expected output:
    val parsed = input.map { case (line : String, x : String) =>
      val t = x.split(" ").map { _.toDouble }
      (t(0),t(1))
    }
    val big = parsed.filter( _._1 > 0.5 )
    val small = parsed.filter( _._1 <= 0.5 )
    val golden = (big ++ small).groupBy{ _._1 }.mapValues { itup => (itup.map{ _._2 }.max) }
    //Now we have the expected input and output:
    JobTest("com.twitter.scalding.MergeTestJob").
      arg("in","fakeInput").
      arg("out","fakeOutput").
      source(TextLine("fakeInput"), input).
      sink[(Double,Double)](Tsv("fakeOutput")) { outBuf =>
        "correctly merge two pipes" in {
          golden must be_==(outBuf.toMap)
        }
      }.
      run.
      finish
  }
}

class SizeAveStdJob(args : Args) extends Job(args) {
  TextLine(args("input")).mapTo('x,'y) { line =>
    val p = line.split(" ").map { _.toDouble }.slice(0,2)
    (p(0),p(1))
  }.map('x -> 'x) { (x : Double) => (4 * x).toInt }
  .groupBy('x) {
    _.sizeAveStdev('y->('size,'yave,'ystdev))
    //Make sure this doesn't ruin the calculation
    .sizeAveStdev('y->('size2,'yave2,'ystdev2))
    .average('y)
  }
  .project('x,'size,'yave,'ystdev,'y)
  .write(Tsv(args("output")))
}

class SizeAveStdSpec extends Specification {
  "A sizeAveStd job" should {
    "correctly compute aves and standard deviations" in {
      val r = new java.util.Random
      def powerLawRand = {
        // Generates a 1/x powerlaw with a max value or 1e40
        scala.math.pow(1e40, r.nextDouble)
      }
      //Here is our input data:
      val input = (0 to 10000).map { i => (i.toString, r.nextDouble.toString +" "+ powerLawRand.toString) }
      val output = input.map { numline => numline._2.split(" ").map { _.toDouble } }
        .map { vec => ((vec(0)*4).toInt, vec(1)) }
        .groupBy { tup => tup._1 }
        .mapValues { tups =>
          val all = tups.map { tup => tup._2.toDouble }.toList
          val size = all.size.toLong
          val ave = all.sum / size
          //Compute the standard deviation:
          val vari = all.map { x => (x-ave)*(x-ave) }.sum / (size)
          val stdev = scala.math.sqrt(vari)
          (size, ave, stdev)
        }
      JobTest(new SizeAveStdJob(_)).
        arg("input","fakeInput").
        arg("output","fakeOutput").
        source(TextLine("fakeInput"), input).
        sink[(Int,Long,Double,Double,Double)](Tsv("fakeOutput")) { outBuf =>
          "correctly compute size, ave, stdev" in {
            outBuf.foreach { computed =>
              val correctTup = output(computed._1)
              //Size
              computed._2 must be_== (correctTup._1)
              //Ave
              computed._3/correctTup._2 must beCloseTo(1.0, 1e-6)
              //Stdev
              computed._4/correctTup._3 must beCloseTo(1.0, 1e-6)
              //Explicitly calculated Average:
              computed._5/computed._3 must beCloseTo(1.0, 1e-6)
            }
          }
        }.
        run.
        finish
    }
  }
}

class DoubleGroupJob(args : Args) extends Job(args) {
  TextLine(args("in")).mapTo('x, 'y) { line =>
      val p = line.split(" ")
      (p(0),p(1))
    }
    .groupBy('x) { _.size }
    .groupBy('size ) { _.size('cnt) }
    .write(Tsv(args("out")))
}

class DoubleGroupSpec extends Specification {
  "A DoubleGroupJob" should {
    "correctly generate output" in {
      JobTest("com.twitter.scalding.DoubleGroupJob").
        arg("in","fakeIn").
        arg("out","fakeOut").
        source(TextLine("fakeIn"), List("0" -> "one 1",
                                        "1" -> "two 1",
                                        "2" -> "two 2",
                                        "3" -> "three 3",
                                        "4" -> "three 4",
                                        "5" -> "three 5",
                                        "6" -> "just one"
                                        )).
        sink[(Long,Long)](Tsv("fakeOut")) { outBuf =>
          "correctly build histogram" in {
            val outM = outBuf.toMap
            outM(1) must be_== (2) //both one and just keys occur only once
            outM(2) must be_== (1)
            outM(3) must be_== (1)
          }
        }.
        run.
        finish
    }
  }
}

class GroupUniqueJob(args : Args) extends Job(args) {
  TextLine(args("in")).mapTo('x, 'y) { line =>
      val p = line.split(" ")
      (p(0),p(1))
    }
    .groupBy('x) { _.size }
    .unique('size )
    .write(Tsv(args("out")))
}

class GroupUniqueSpec extends Specification {
  "A GroupUniqueJob" should {
    JobTest("com.twitter.scalding.GroupUniqueJob").
      arg("in","fakeIn").
      arg("out","fakeOut").
      source(TextLine("fakeIn"), List("0" -> "one 1",
                                      "1" -> "two 1",
                                      "2" -> "two 2",
                                      "3" -> "three 3",
                                      "4" -> "three 4",
                                      "5" -> "three 5",
                                      "6" -> "just one"
                                      )).
      sink[(Long)](Tsv("fakeOut")) { outBuf =>
        "correctly count unique sizes" in {
          val outSet = outBuf.toSet
          outSet.size must_== 3
        }
      }.
      run.
      finish
  }
}

class DiscardTestJob(args : Args) extends Job(args) {
  TextLine(args("in")).flatMapTo('words) { line => line.split("\\s+") }
    .map('words -> 'wsize) { word : String => word.length }
    .discard('words)
    .map('* -> 'correct) { te : TupleEntry => !te.getFields.contains('words) }
    .groupAll { _.forall('correct -> 'correct) { x : Boolean => x } }
    .write(Tsv(args("out")))
}

class DiscardTest extends Specification {
  "A DiscardTestJob" should {
    JobTest("com.twitter.scalding.DiscardTestJob")
      .arg("in","fakeIn")
      .arg("out","fakeOut")
      .source(TextLine("fakeIn"), List("0" -> "hello world", "1" -> "foo", "2" -> "bar"))
      .sink[Boolean](Tsv("fakeOut")) { outBuf =>
        "must reduce down to one line" in {
          outBuf.size must_== 1
        }
        "must correctly discard word column" in {
          outBuf(0) must beTrue
        }
      }
      .run
      .finish
  }
}

class HistogramJob(args : Args) extends Job(args) {
  TextLine(args("in")).read
    .groupBy('line) { _.size }
    .groupBy('size) { _.size('freq) }
    .write(Tsv(args("out")))
}

class HistogramTest extends Specification {
  "A HistogramJob" should {
    JobTest("com.twitter.scalding.HistogramJob")
      .arg("in","fakeIn")
      .arg("out","fakeOut")
      .source(TextLine("fakeIn"), List("0" -> "single", "1" -> "single"))
      .sink[(Long,Long)](Tsv("fakeOut")) { outBuf =>
        "must reduce down to a single line for a trivial input" in {
          outBuf.size must_== 1
        }
        "must get the result right" in {
          outBuf(0) must_== (2L,1L)
        }
      }
      .run
      .finish
  }
}

class ForceReducersJob(args : Args) extends Job(args) {
  TextLine("in").read
    .rename((0, 1) -> ('num, 'line))
    .flatMap('line -> 'words){l : String => l.split(" ")}
    .groupBy('num){ _.toList[String]('words -> 'wordList).forceToReducers }
    .map('wordList -> 'wordList){w : List[String] => w.mkString(" ")}
    .project('num, 'wordList)
    .write(Tsv("out"))
}

class ForceReducersTest extends Specification {
  "A ForceReducersJob" should {
    JobTest("com.twitter.scalding.ForceReducersJob")
      .source(TextLine("in"), List("0" -> "single test", "1" -> "single result"))
      .sink[(Int,String)](Tsv("out")) { outBuf =>
        "must get the result right" in {
          //need to convert to sets because order
          outBuf(0)._2.split(" ").toSet must_== Set("single", "test")
          outBuf(1)._2.split(" ").toSet must_== Set("single", "result")
        }
      }
      .run
      .runHadoop
      .finish
  }
}

class ToListJob(args : Args) extends Job(args) {
  TextLine(args("in")).read
    .flatMap('line -> 'words){l : String => l.split(" ")}
    .groupBy('offset){ _.toList[String]('words -> 'wordList) }
    .map('wordList -> 'wordList){w : List[String] => w.mkString(" ")}
    .project('offset, 'wordList)
    .write(Tsv(args("out")))
}

class NullListJob(args : Args) extends Job(args) {
  TextLine(args("in")).read
    .groupBy('offset){ _.toList[String]('line -> 'lineList).spillThreshold(100) }
    .map('lineList -> 'lineList) { ll : List[String] => ll.mkString(" ") }
    .write(Tsv(args("out")))
}

class ToListTest extends Specification {
  "A ToListJob" should {
    JobTest("com.twitter.scalding.ToListJob")
      .arg("in","fakeIn")
      .arg("out","fakeOut")
      .source(TextLine("fakeIn"), List("0" -> "single test", "1" -> "single result"))
      .sink[(Int,String)](Tsv("fakeOut")) { outBuf =>
        "must have the right number of lines" in {
          outBuf.size must_== 2
        }
        "must get the result right" in {
          //need to convert to sets because order
          outBuf(0)._2.split(" ").toSet must_== Set("single", "test")
          outBuf(1)._2.split(" ").toSet must_== Set("single", "result")
        }
      }
      .run
      .finish
  }

  "A NullListJob" should {
    JobTest("com.twitter.scalding.NullListJob")
      .arg("in","fakeIn")
      .arg("out","fakeOut")
      .source(TextLine("fakeIn"), List("0" -> null, "0" -> "a", "0" -> null, "0" -> "b"))
      .sink[(Int,String)](Tsv("fakeOut")) { outBuf =>
        "must have the right number of lines" in {
          outBuf.size must_== 1
        }
        "must return an empty list for null key" in {
          val sSet = outBuf(0)._2.split(" ").toSet
          sSet must_== Set("a", "b")
        }
      }
      .run
      .finish
  }
}

class CrossJob(args : Args) extends Job(args) {
  val p1 = Tsv(args("in1")).read
    .mapTo((0,1) -> ('x,'y)) { tup : (Int, Int) => tup }
  val p2 = Tsv(args("in2")).read
    .mapTo(0->'z) { (z : Int) => z}
  p1.crossWithTiny(p2).write(Tsv(args("out")))
}

class CrossTest extends Specification {
  noDetailedDiffs()

  "A CrossJob" should {
    JobTest("com.twitter.scalding.CrossJob")
      .arg("in1","fakeIn1")
      .arg("in2","fakeIn2")
      .arg("out","fakeOut")
      .source(Tsv("fakeIn1"), List(("0","1"),("1","2"),("2","3")))
      .source(Tsv("fakeIn2"), List("4","5").map { Tuple1(_) })
      .sink[(Int,Int,Int)](Tsv("fakeOut")) { outBuf =>
        "must look exactly right" in {
          outBuf.size must_==6
          outBuf.toSet must_==(Set((0,1,4),(0,1,5),(1,2,4),(1,2,5),(2,3,4),(2,3,5)))
        }
      }
      .run
      .runHadoop
      .finish
  }
}

class GroupAllCrossJob(args : Args) extends Job(args) {
  val p1 = Tsv(args("in1")).read
    .mapTo((0,1) -> ('x,'y)) { tup : (Int, Int) => tup }
    .groupAll { _.max('x) }
    .map('x -> 'x) { x : Int => List(x) }

  val p2 = Tsv(args("in2")).read
    .mapTo(0->'z) { (z : Int) => z}
  p2.crossWithTiny(p1)
    .map('x -> 'x) { l: List[Int] => l.size }
    .project('x, 'z)
    .write(Tsv(args("out")))
}

class GroupAllCrossTest extends Specification {
  noDetailedDiffs()

  "A GroupAllCrossJob" should {
    JobTest(new GroupAllCrossJob(_))
      .arg("in1","fakeIn1")
      .arg("in2","fakeIn2")
      .arg("out","fakeOut")
      .source(Tsv("fakeIn1"), List(("0","1"),("1","2"),("2","3")))
      .source(Tsv("fakeIn2"), List("4","5").map { Tuple1(_) })
      .sink[(Int,Int)](Tsv("fakeOut")) { outBuf =>
        "must look exactly right" in {
          outBuf.size must_==2
          outBuf.toSet must_==(Set((1,4), (1,5)))
        }
      }
      .run
      .runHadoop
      .finish
  }
}

class SmallCrossJob(args : Args) extends Job(args) {
  val p1 = Tsv(args("in1")).read
    .mapTo((0,1) -> ('x,'y)) { tup : (Int, Int) => tup }
  val p2 = Tsv(args("in2")).read
    .mapTo(0->'z) { (z : Int) => z}
  p1.crossWithSmaller(p2).write(Tsv(args("out")))
}

class SmallCrossTest extends Specification {
  noDetailedDiffs()

  "A SmallCrossJob" should {
    JobTest("com.twitter.scalding.SmallCrossJob")
      .arg("in1","fakeIn1")
      .arg("in2","fakeIn2")
      .arg("out","fakeOut")
      .source(Tsv("fakeIn1"), List(("0","1"),("1","2"),("2","3")))
      .source(Tsv("fakeIn2"), List("4","5").map { Tuple1(_) })
      .sink[(Int,Int,Int)](Tsv("fakeOut")) { outBuf =>
        "must look exactly right" in {
          outBuf.size must_==6
          outBuf.toSet must_==(Set((0,1,4),(0,1,5),(1,2,4),(1,2,5),(2,3,4),(2,3,5)))
        }
      }
      .run
      .runHadoop
      .finish
  }
}

class TopKJob(args : Args) extends Job(args) {
  Tsv(args("in")).read
    .mapTo(0 -> 'x) { (tup : Int) => tup }
    //Take the smallest 3 values:
    .groupAll { _.sortedTake[Int]('x->'x, 3) }
    .write(Tsv(args("out")))
}

class TopKTest extends Specification {
  "A TopKJob" should {
    JobTest("com.twitter.scalding.TopKJob")
      .arg("in","fakeIn")
      .arg("out","fakeOut")
      .source(Tsv("fakeIn"), List(3,24,1,4,5).map { Tuple1(_) } )
      .sink[List[Int]](Tsv("fakeOut")) { outBuf =>
        "must look exactly right" in {
          outBuf.size must_==1
          outBuf(0) must be_==(List(1,3,4))
        }
      }
      .run
      .finish
  }
}

class ScanJob(args : Args) extends Job(args) {
  Tsv("in",('x,'y,'z))
    .groupBy('x) {
      _.sortBy('y)
        .scanLeft('y -> 'ys)(0) { (oldV : Int, newV : Int) => oldV + newV }
    }
    .project('x,'ys,'z)
    .map('z -> 'z) { z : Int => z } //Make sure the null z is converted to an int
    .write(Tsv("out"))
}

class ScanTest extends Specification {
  import Dsl._
  noDetailedDiffs()
  "A ScanJob" should {
    JobTest("com.twitter.scalding.ScanJob")
      .source(Tsv("in",('x,'y,'z)), List((3,0,1),(3,1,10),(3,5,100)) )
      .sink[(Int,Int,Int)](Tsv("out")) { outBuf => ()
        val correct = List((3,0,0),(3,0,1),(3,1,10),(3,6,100))
        "have a working scanLeft" in {
          outBuf.toList must be_== (correct)
        }
      }
      .run
      .runHadoop
      .finish
  }
}

class TakeJob(args : Args) extends Job(args) {
  val input = Tsv("in").read
    .mapTo((0,1,2) -> ('x,'y,'z)) { tup : (Int,Int,Int) => tup }

  input.groupBy('x) { _.take(2) }.write(Tsv("out2"))
  input.groupAll.write(Tsv("outall"))
}

class TakeTest extends Specification {
  noDetailedDiffs()
  "A TakeJob" should {
    JobTest("com.twitter.scalding.TakeJob")
      .source(Tsv("in"), List((3,0,1),(3,1,10),(3,5,100)) )
      .sink[(Int,Int,Int)](Tsv("outall")) { outBuf => ()
        "groupAll must see everything in same order" in {
          outBuf.size must_==3
          outBuf.toList must be_== (List((3,0,1),(3,1,10),(3,5,100)))
        }
      }
      .sink[(Int,Int,Int)](Tsv("out2")) { outBuf =>
        "take(2) must only get 2" in {
          outBuf.size must_==2
          outBuf.toList must be_== (List((3,0,1),(3,1,10)))
        }
      }
      .run
      .finish
  }
}

class DropJob(args : Args) extends Job(args) {
  val input = Tsv("in").read
    .mapTo((0,1,2) -> ('x,'y,'z)) { tup : (Int,Int,Int) => tup }

  input.groupBy('x) { _.drop(2) }.write(Tsv("out2"))
  input.groupAll.write(Tsv("outall"))
}

class DropTest extends Specification {
  noDetailedDiffs()
  "A DropJob" should {
    JobTest("com.twitter.scalding.DropJob")
      .source(Tsv("in"), List((3,0,1),(3,1,10),(3,5,100)) )
      .sink[(Int,Int,Int)](Tsv("outall")) { outBuf => ()
        "groupAll must see everything in same order" in {
          outBuf.size must_==3
          outBuf.toList must be_== (List((3,0,1),(3,1,10),(3,5,100)))
        }
      }
      .sink[(Int,Int,Int)](Tsv("out2")) { outBuf =>
        "drop(2) must only get 1" in {
          outBuf.toList must be_== (List((3,5,100)))
        }
      }
      .run
      .finish
  }
}

class PivotJob(args : Args) extends Job(args) {
  Tsv("in",('k,'w,'y,'z)).read
    .unpivot(('w,'y,'z) -> ('col, 'val))
    .write(Tsv("unpivot"))
    .groupBy('k) {
      _.pivot(('col,'val) -> ('w,'y,'z))
    }.write(Tsv("pivot"))
    .unpivot(('w,'y,'z) -> ('col, 'val))
    .groupBy('k) {
      _.pivot(('col,'val) -> ('w,'y,'z,'default), 2.0)
    }.write(Tsv("pivot_with_default"))
}

class PivotTest extends Specification with FieldConversions {
  noDetailedDiffs()
  val input = List(("1","a","b","c"),("2","d","e","f"))
  "A PivotJob" should {
    JobTest("com.twitter.scalding.PivotJob")
      .source(Tsv("in",('k,'w,'y,'z)), input)
      .sink[(String,String,String)](Tsv("unpivot")) { outBuf =>
        "unpivot columns correctly" in {
          outBuf.size must_== 6
          outBuf.toList.sorted must be_== (List(("1","w","a"),("1","y","b"),("1","z","c"),
            ("2","w","d"),("2","y","e"),("2","z","f")).sorted)
        }
      }
      .sink[(String,String,String,String)](Tsv("pivot")) { outBuf =>
        "pivot back to the original" in {
          outBuf.size must_==2
          outBuf.toList.sorted must be_== (input.sorted)
        }
      }
      .sink[(String,String,String,String,Double)](Tsv("pivot_with_default")) { outBuf =>
        "pivot back to the original with the missing column replace by the specified default" in {
          outBuf.size must_==2
          outBuf.toList.sorted must be_== (List(("1","a","b","c",2.0),("2","d","e","f",2.0)).sorted)
        }
      }
      .run
      .finish
  }
}

class IterableSourceJob(args : Args) extends Job(args) {
  val list = List((1,2,3),(4,5,6),(3,8,9))
  val iter = IterableSource(list, ('x,'y,'z))
  Tsv("in",('x,'w))
    .joinWithSmaller('x->'x, iter)
    .write(Tsv("out"))

  Tsv("in",('x,'w))
    .joinWithTiny('x->'x, iter)
    .write(Tsv("tiny"))
  //Now without fields and using the implicit:
  Tsv("in",('x,'w))
    .joinWithTiny('x -> 0, list).write(Tsv("imp"))
}

class IterableSourceTest extends Specification with FieldConversions {
  noDetailedDiffs()
  val input = List((1,10),(2,20),(3,30))
  "A IterableSourceJob" should {
    JobTest("com.twitter.scalding.IterableSourceJob")
      .source(Tsv("in",('x,'w)), input)
      .sink[(Int,Int,Int,Int)](Tsv("out")) { outBuf =>
        "Correctly joinWithSmaller" in {
          outBuf.toList.sorted must be_== (List((1,10,2,3),(3,30,8,9)))
        }
      }
      .sink[(Int,Int,Int,Int)](Tsv("tiny")) { outBuf =>
        "Correctly joinWithTiny" in {
          outBuf.toList.sorted must be_== (List((1,10,2,3),(3,30,8,9)))
        }
      }
      .sink[(Int,Int,Int,Int,Int)](Tsv("imp")) { outBuf =>
        "Correctly implicitly joinWithTiny" in {
          outBuf.toList.sorted must be_== (List((1,10,1,2,3),(3,30,3,8,9)))
        }
      }
      .run
      .runHadoop
      .finish
  }
}

class HeadLastJob(args : Args) extends Job(args) {
  Tsv("input",('x,'y)).groupBy('x) {
    _.sortBy('y)
      .head('y -> 'yh).last('y -> 'yl)
  }.write(Tsv("output"))
}

class HeadLastTest extends Specification {
  import Dsl._
  noDetailedDiffs()
  val input = List((1,10),(1,20),(1,30),(2,0))
  "A HeadLastJob" should {
    JobTest("com.twitter.scalding.HeadLastJob")
      .source(Tsv("input",('x,'y)), input)
      .sink[(Int,Int,Int)](Tsv("output")) { outBuf =>
        "Correctly do head/last" in {
          outBuf.toList must be_==(List((1,10,30),(2,0,0)))
        }
      }
      .run
      .finish
  }
}

class HeadLastUnsortedJob(args : Args) extends Job(args) {
  Tsv("input",('x,'y)).groupBy('x) {
    _.head('y -> 'yh).last('y -> 'yl)
  }.write(Tsv("output"))
}

class HeadLastUnsortedTest extends Specification {
  import Dsl._
  noDetailedDiffs()
  val input = List((1,10),(1,20),(1,30),(2,0))
  "A HeadLastUnsortedTest" should {
    JobTest("com.twitter.scalding.HeadLastUnsortedJob")
      .source(Tsv("input",('x,'y)), input)
      .sink[(Int,Int,Int)](Tsv("output")) { outBuf =>
        "Correctly do head/last" in {
          outBuf.toList must be_==(List((1,10,30),(2,0,0)))
        }
      }
      .run
      .finish
  }
}

class MkStringToListJob(args : Args) extends Job(args) {
  Tsv("input", ('x,'y)).groupBy('x) {
    _.sortBy('y)
      .mkString('y -> 'ystring,",")
      .toList[Int]('y -> 'ylist)
  }.write(Tsv("output"))
}

class MkStringToListTest extends Specification with FieldConversions {
  noDetailedDiffs()
  val input = List((1,30),(1,10),(1,20),(2,0))
  "A IterableSourceJob" should {
    JobTest("com.twitter.scalding.MkStringToListJob")
      .source(Tsv("input",('x,'y)), input)
      .sink[(Int,String,List[Int])](Tsv("output")) { outBuf =>
        "Correctly do mkString/toList" in {
          outBuf.toSet must be_==(Set((1,"10,20,30",List(10,20,30)),(2,"0",List(0))))
        }
      }
      .run
      // This can't be run in Hadoop mode because we can't serialize the list to Tsv
      .finish
  }
}

class InsertJob(args : Args) extends Job(args) {
  Tsv("input", ('x, 'y)).insert(('z, 'w), (1,2)).write(Tsv("output"))
}

class InsertJobTest extends Specification {
  import Dsl._
  noDetailedDiffs()

  val input = List((2,2), (3,3))

  "An InsertJob" should {
    JobTest(new com.twitter.scalding.InsertJob(_))
      .source(Tsv("input", ('x, 'y)), input)
      .sink[(Int, Int, Int, Int)](Tsv("output")) { outBuf =>
        "Correctly insert a constant" in {
          outBuf.toSet must be_==(Set((2,2,1,2), (3,3,1,2)))
        }
      }
      .run
      .finish
  }
}

class FoldJob(args : Args) extends Job(args) {
  import scala.collection.mutable.{Set => MSet}
  Tsv("input", ('x,'y)).groupBy('x) {
      // DON'T USE MUTABLE, IT IS UNCOOL AND DANGEROUS!, but we test, just in case
      _.foldLeft('y -> 'yset)(MSet[Int]()){(ms : MSet[Int], y : Int) =>
        ms += y
        ms
      }
  }.write(Tsv("output"))
}

class FoldJobTest extends Specification {
  import Dsl._
  import scala.collection.mutable.{Set => MSet}

  noDetailedDiffs()
  val input = List((1,30),(1,10),(1,20),(2,0))
  "A FoldTestJob" should {
    JobTest("com.twitter.scalding.FoldJob")
      .source(Tsv("input",('x,'y)), input)
      .sink[(Int,MSet[Int])](Tsv("output")) { outBuf =>
        "Correctly do a fold with MutableSet" in {
          outBuf.toSet must be_==(Set((1,MSet(10,20,30)),(2,MSet(0))))
        }
      }
      .run
      // This can't be run in Hadoop mode because we can't serialize the list to Tsv
      .finish
  }
}

// TODO make a Product serializer that clean $outer parameters
case class V(v : Int)
class InnerCaseJob(args : Args) extends Job(args) {
 val res = TypedTsv[Int]("input")
   .mapTo(('xx, 'vx)) { x => (x*x, V(x)) }
   .groupBy('xx) { _.head('vx) }
   .map('vx -> 'x) { v : V => v.v }
   .project('x, 'xx)
   .write(Tsv("output"))
}

class InnerCaseTest extends Specification {
  import Dsl._

  noDetailedDiffs()
  val input = List(Tuple1(1),Tuple1(2),Tuple1(2),Tuple1(4))
  "An InnerCaseJob" should {
    JobTest(new com.twitter.scalding.InnerCaseJob(_))
      .source(TypedTsv[Int]("input"), input)
      .sink[(Int,Int)](Tsv("output")) { outBuf =>
        "Correctly handle inner case classes" in {
          outBuf.toSet must be_==(Set((1,1),(2,4),(4,16)))
        }
      }
      .runHadoop
      .finish
  }
}

class NormalizeJob(args : Args) extends Job(args) {
  Tsv("in")
    .read
    .mapTo((0,1) -> ('x,'y)) { tup : (Double, Int) => tup }
    .normalize('x)
    .project('x, 'y)
    .write(Tsv("out"))
}

class NormalizeTest extends Specification {
  noDetailedDiffs()

  "A NormalizeJob" should {
    JobTest("com.twitter.scalding.NormalizeJob")
      .source(Tsv("in"), List(("0.3", "1"), ("0.3", "1"), ("0.3",
"1"), ("0.3", "1")))
      .sink[(Double, Int)](Tsv("out")) { outBuf =>
        "must be normalized" in {
          outBuf.size must_== 4
          outBuf.toSet must_==(Set((0.25,1),(0.25,1),(0.25,1),(0.25,1)))
        }
      }
      .run
      .finish
  }
}

class ApproxUniqJob(args : Args) extends Job(args) {
  Tsv("in",('x,'y))
    .read
    .groupBy('x) { _.approxUniques('y -> 'ycnt) }
    .write(Tsv("out"))
}

class ApproxUniqTest extends Specification {
  import Dsl._
  noDetailedDiffs()

  "A ApproxUniqJob" should {
    val input = (1 to 1000).flatMap { i => List(("x0", i), ("x1", i)) }.toList
    JobTest("com.twitter.scalding.ApproxUniqJob")
      .source(Tsv("in",('x,'y)), input)
      .sink[(String, Double)](Tsv("out")) { outBuf =>
        "must approximately count" in {
          outBuf.size must_== 2
          val kvresult = outBuf.groupBy { _._1 }.mapValues { _.head._2 }
          kvresult("x0") must beCloseTo(1000.0, 30.0) //We should be 1%, but this is on average, so
          kvresult("x1") must beCloseTo(1000.0, 30.0) //We should be 1%, but this is on average, so
        }
      }
      .run
      .finish
  }
}

class ForceToDiskJob(args : Args) extends Job(args) {
  val x = Tsv("in", ('x,'y))
    .read
    .filter('x) { x : Int => x > 0 }
    .rename('x -> 'x1)
  Tsv("in",('x,'y))
    .read
    .joinWithTiny('y -> 'y, x.forceToDisk)
    .project('x,'x1,'y)
    .write(Tsv("out"))
}

class ForceToDiskTest extends Specification {
  import Dsl._
  noDetailedDiffs()

  "A ForceToDiskJob" should {
    val input = (1 to 1000).flatMap { i => List((-1, i), (1, i)) }.toList
    JobTest(new ForceToDiskJob(_))
      .source(Tsv("in",('x,'y)), input)
      .sink[(Int,Int,Int)](Tsv("out")) { outBuf =>
        "run correctly when combined with joinWithTiny" in {
          outBuf.size must_== 2000
          val correct = (1 to 1000).flatMap { y => List((1,1,y),(-1,1,y)) }.sorted
          outBuf.toList.sorted must_== correct
        }
      }
      .run
      .runHadoop
      .finish
  }
}

class ThrowsErrorsJob(args : Args) extends Job(args) {
  Tsv("input",('letter, 'x))
    .read
    .addTrap(Tsv("trapped"))
    .map(('letter, 'x) -> 'yPrime){ fields : Product =>
        val x = fields.productElement(1).asInstanceOf[Int]
        if (x == 1) throw new Exception("Erroneous Ones") else x }
    .write(Tsv("output"))
}


class ItsATrapTest extends Specification {
  import Dsl._

  noDetailedDiffs() //Fixes an issue with scala 2.9
  "An AddTrap" should {
    val input = List(("a", 1),("b", 2), ("c", 3), ("d", 1), ("e", 2))

    JobTest(new ThrowsErrorsJob(_))
      .source(Tsv("input",('letter,'x)), input)
      .sink[(String, Int)](Tsv("output")) { outBuf =>
        "must contain all numbers in input except for 1" in {
          outBuf.toList.sorted must be_==(List(("b", 2), ("c", 3), ("e", 2)))
        }
      }
      .sink[(String, Int)](Tsv("trapped")) { outBuf =>
        "must contain all 1s and fields in input" in {
          outBuf.toList.sorted must be_==(List(("a", 1), ("d", 1)))
        }
      }
      .run
      .finish
  }
}

class GroupAllToListTestJob(args: Args) extends Job(args) {
  TypedTsv[(Long, String, Double)]("input")
    .mapTo('a, 'b) { case(id, k, v) => (id, Map(k -> v)) }
    .groupBy('a) { _.sum[Map[String, Double]]('b) }
    .groupAll {
      _.toList[(Long, Map[String, Double])](('a, 'b) -> 'abList)
    }
    .map('abList -> 'abMap) {
      list : List[(Long, Map[String, Double])] => list.toMap
    }
    .project('abMap)
    .map('abMap -> 'abMap) { x: AnyRef => x.toString }
    .write(Tsv("output"))
}

class GroupAllToListTest extends Specification {
  import Dsl._

  noDetailedDiffs()

  "A GroupAllToListTestJob" should {
    val input = List((1L, "a", 1.0), (1L, "b", 2.0), (2L, "a", 1.0), (2L, "b", 2.0))
    val output = Map(2L -> Map("a" -> 1.0, "b" -> 2.0), 1L -> Map("a" -> 1.0, "b" -> 2.0))
    JobTest(new GroupAllToListTestJob(_))
      .source(TypedTsv[(Long, String, Double)]("input"), input)
      .sink[String](Tsv("output")) { outBuf =>
        "must properly aggregate stuff into a single map" in {
          outBuf.size must_== 1
          outBuf(0) must be_==(output.toString)
        }
      }
      .runHadoop
      .finish
  }
}

class ToListGroupAllToListTestJob(args: Args) extends Job(args) {
  TypedTsv[(Long, String)]("input")
    .mapTo('b, 'c) { case(k, v) => (k, v) }
    .groupBy('c) { _.toList[Long]('b -> 'bList) }
    .groupAll {
      _.toList[(String, List[Long])](('c, 'bList) -> 'cbList)
    }
    .project('cbList)
    .write(Tsv("output"))
}

class ToListGroupAllToListSpec extends Specification {
  import Dsl._

  noDetailedDiffs()

  val expected = List(("us", List(1)), ("jp", List(3, 2)), ("gb", List(3, 1)))

  "A ToListGroupAllToListTestJob" should {
    JobTest(new ToListGroupAllToListTestJob(_))
      .source(TypedTsv[(Long, String)]("input"), List((1L, "us"), (1L, "gb"), (2L, "jp"), (3L, "jp"), (3L, "gb")))
      .sink[String](Tsv("output")) { outBuf =>
        "must properly aggregate stuff in hadoop mode" in {
          outBuf.size must_== 1
          outBuf.head must_== expected.toString
          println(outBuf.head)
        }
      }
      .runHadoop
      .finish

    JobTest(new ToListGroupAllToListTestJob(_))
      .source(TypedTsv[(Long, String)]("input"), List((1L, "us"), (1L, "gb"), (2L, "jp"), (3L, "jp"), (3L, "gb")))
      .sink[List[(String, List[Long])]](Tsv("output")) { outBuf =>
        "must properly aggregate stuff in local model" in {
          outBuf.size must_== 1
          outBuf.head must_== expected
          println(outBuf.head)
        }
      }
      .run
      .finish
  }
}

// TODO: HangingTest is very flaky now because we enabled multi-thread testing. Need to be fixed later.
/*
class HangingJob(args : Args) extends Job(args) {
  val x = Tsv("in", ('x,'y))
    .read
    .filter('x, 'y) { t: (Int, Int) =>
      val (x, y) = t
      timeout(Millisecs(2)) {
        if (y % 2 == 1) Thread.sleep(1000)
        x > 0
      } getOrElse false
    }
    .write(Tsv("out"))
}

class HangingTest extends Specification {
  import Dsl._
  noDetailedDiffs()

  "A HangingJob" should {
    val input = (1 to 100).flatMap { i => List((-1, i), (1, i)) }.toList
    JobTest(new HangingJob(_))
      .source(Tsv("in",('x,'y)), input)
      .sink[(Int,Int)](Tsv("out")) { outBuf =>
        "run correctly when task times out" in {
          //outBuf.size must_== 100
          //val correct = (1 to 100).map { i => (1, i) }
          outBuf.size must_== 50
          val correct = (1 to 50).map { i => (1, i*2) }
          outBuf.toList.sorted must_== correct
        }
      }
      .run
      .runHadoop
      .finish
  }
}
*/

class Function2Job(args : Args) extends Job(args) {
  import FunctionImplicits._
  Tsv("in", ('x,'y)).mapTo(('x, 'y) -> 'xy) { (x: String, y: String) => x + y }.write(Tsv("output"))
}

class Function2Test extends Specification {
  import Dsl._
  noDetailedDiffs() //Fixes an issue with scala 2.9
  "A Function2Job" should {
    val input = List(("a", "b"))

    JobTest("com.twitter.scalding.Function2Job")
      .source(Tsv("in",('x,'y)), input)
      .sink[String](Tsv("output")) { outBuf =>
        "convert a function2 to tupled function1" in {
          outBuf must be_==(List("ab"))
        }
      }
      .run
      .finish
  }
}
