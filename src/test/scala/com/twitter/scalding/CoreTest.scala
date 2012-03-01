package com.twitter.scalding

import cascading.tuple.Fields
import cascading.tuple.TupleEntry

import org.specs._

class NumberJoinerJob(args : Args) extends Job(args) {
  val in0 = Tsv("input0").read.mapTo((0,1) -> ('x0, 'y0)) { input : (Int, Int) => input }
  val in1 = Tsv("input1").read.mapTo((0,1) -> ('x1, 'y1)) { input : (Long, Long) => input }
  in0.joinWithSmaller('x0 -> 'x1, in1)
  .write(Tsv("output"))
}

class NumberJoinTest extends Specification with TupleConversions {
  "A NumberJoinerJob" should {
    //Set up the job:
    "not throw when joining longs with ints" in {
      JobTest("com.twitter.scalding.NumberJoinerJob")
        .source(Tsv("input0"), List(("0","1"), ("1","2"), ("2","4")))
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

class MapToGroupBySizeSumMaxJob(args: Args) extends Job(args) {
  TextLine(args("input")).read.
  //1 is the line
  mapTo(1-> ('kx,'x)) { line : String =>
    val x = line.toDouble
    ((x > 0.5),x)
  }.
  groupBy('kx) { _.size.sum('x->'sx).max('x) }.
  write( Tsv(args("output")) )
}

class MapToGroupBySizeSumMaxTest extends Specification with TupleConversions {
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

class JoinTest extends Specification with TupleConversions {
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

class TinyJoinTest extends Specification with TupleConversions {
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
      .runHadoop
      .finish
  }
}

class TinyThenSmallJoin(args : Args) extends Job(args) {
  val pipe0 = Tsv("in0",('x0,'y0)).read
  val pipe1 = Tsv("in1",('x1,'y1)).read
  val pipe2 = Tsv("in2",('x2,'y2)).read

  pipe0.joinWithTiny('x0 -> 'x1, pipe1)
    .joinWithSmaller('x0 -> 'x2, pipe2)
    .write(Tsv("out"))
}

class TinyThenSmallJoinTest extends Specification with TupleConversions with FieldConversions {
  noDetailedDiffs() //Fixes an issue with scala 2.9
  "A TinyThenSmallJoin" should {
    val input0 = List((1,2),(2,3),(3,4))
    val input1 = List((1,20),(2,30),(3,40))
    val input2 = List((1,200),(2,300),(3,400))
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

class MergeTest extends Specification with TupleConversions {
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

class SizeAveStdSpec extends Specification with TupleConversions {
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
          val vari = all.map { x => (x-ave)*(x-ave) }.sum / (size - 1)
          val stdev = scala.math.sqrt(vari)
          (size, ave, stdev)
        }
      JobTest("com.twitter.scalding.SizeAveStdJob").
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

class DoubleGroupSpec extends Specification with TupleConversions {
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

class GroupUniqueSpec extends Specification with TupleConversions {
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
    .map(Fields.ALL -> 'correct) { te : TupleEntry => !te.getFields.contains('words) }
    .groupAll { _.forall('correct -> 'correct) { x : Boolean => x } }
    .write(Tsv(args("out")))
}

class DiscardTest extends Specification with TupleConversions {
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

class HistogramTest extends Specification with TupleConversions {
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

class ToListJob(args : Args) extends Job(args) {
  TextLine(args("in")).read
    .flatMap('line -> 'words){l : String => l.split(" ")}
    .groupBy('num){ _.toList[String]('words -> 'wordList) }
    .map('wordList -> 'wordList){w : List[String] => w.mkString(" ")}
    .project('num, 'wordList)
    .write(Tsv(args("out")))
}

class NullListJob(args : Args) extends Job(args) {
  TextLine(args("in")).read
    .groupBy('num){ _.toList[String]('line -> 'lineList) }
    .map('lineList -> 'lineList) { ll : List[String] => ll.mkString(" ") }
    .write(Tsv(args("out")))
}

class ToListTest extends Specification with TupleConversions {
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

class CrossTest extends Specification with TupleConversions {
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

class TopKJob(args : Args) extends Job(args) {
  Tsv(args("in")).read
    .mapTo(0 -> 'x) { (tup : Int) => tup }
    //Take the smallest 3 values:
    .groupAll { _.sortedTake[Int]('x->'x, 3) }
    .write(Tsv(args("out")))
}

class TopKTest extends Specification with TupleConversions {
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

class TakeJob(args : Args) extends Job(args) {
  val input = Tsv("in").read
    .mapTo((0,1,2) -> ('x,'y,'z)) { tup : (Int,Int,Int) => tup }

  input.groupBy('x) { _.take(2) }.write(Tsv("out2"))
  input.groupAll.write(Tsv("outall"))
}

class TakeTest extends Specification with TupleConversions {
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
