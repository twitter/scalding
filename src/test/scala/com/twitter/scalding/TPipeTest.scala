package com.twitter.scalding

import org.specs._

import TDsl._

object TUtil {
  def printStack( fn: => Unit ) {
    try { fn } catch { case e : Throwable => e.printStackTrace; throw e }
  }
}

class TPipeJob(args : Args) extends Job(args) {
  //Word count using TPipe
  TextLine("inputFile")
    .flatMap { _.split("\\s+") }
    .map { w => (w, 1L) }
    .sum
    .write(('key, 'value), Tsv("outputFile"))
}

class TPipeTest extends Specification with TupleConversions {
  "A TPipe" should {
    TUtil.printStack {
    JobTest("com.twitter.scalding.TPipeJob").
      arg("input", "inputFile").
      arg("output", "outputFile").
      source(TextLine("inputFile"), List("0" -> "hack hack hack and hack")).
      sink[(String,Int)](Tsv("outputFile")){ outputBuffer =>
        val outMap = outputBuffer.toMap
        "count words correctly" in {
          outMap("hack") must be_==(4)
          outMap("and") must be_==(1)
        }
      }.
      run.
      finish
    }
  }
}

class TPipeJoinJob(args : Args) extends Job(args) {
  (Tsv("inputFile0").read.toTPipe[(Int,Int)](0, 1)
    leftJoin TPipe.from[(Int,Int)](Tsv("inputFile1").read, (0, 1)))
    .toPipe('key, 'value)
    .write(Tsv("outputFile"))
}

class TPipeJoinTest extends Specification with TupleConversions {
  import Dsl._
  "A TPipeJoin" should {
    JobTest("com.twitter.scalding.TPipeJoinJob")
      .source(Tsv("inputFile0"), List((0,0), (1,1), (2,2), (3,3), (4,5)))
      .source(Tsv("inputFile1"), List((0,1), (1,2), (2,3), (3,4)))
      .sink[(Int,(Int,Option[Int]))](Tsv("outputFile")){ outputBuffer =>
        val outMap = outputBuffer.toMap
        "correctly join" in {
          outMap(0) must be_==((0,Some(1)))
          outMap(1) must be_==((1,Some(2)))
          outMap(2) must be_==((2,Some(3)))
          outMap(3) must be_==((3,Some(4)))
          outMap(4) must be_==((5,None))
          outMap.size must be_==(5)
        }
      }.
      run.
      finish
  }
}

class TypedImplicitJob(args : Args) extends Job(args) {
  def revTup[K,V](in : (K,V)) : (V,K) = (in._2, in._1)
  TextLine("inputFile").read.typed(1 -> ('maxWord, 'maxCnt)) { tpipe : TPipe[String] =>
    tpipe.flatMap { _.split("\\s+") }
      .map { w => (w, 1L) }
      // groupby the key and sum the values:
      .sum
      .groupAll
      .mapValues { revTup _ }
      .max
      // Throw out the Unit key and reverse the value tuple
      .map { tup => revTup(tup._2) }
  }.write(Tsv("outputFile"))
}

class TPipeTypedTest extends Specification {
  import Dsl._
  "A TypedImplicitJob" should {
    JobTest("com.twitter.scalding.TypedImplicitJob")
      .source(TextLine("inputFile"), List("0" -> "hack hack hack and hack"))
      .sink[(String,Int)](Tsv("outputFile")){ outputBuffer =>
        val outMap = outputBuffer.toMap
        "find max word" in {
          outMap("hack") must be_==(4)
          outMap.size must be_==(1)
        }
      }
      .run
      .finish
  }
}

class TJoinCountJob(args : Args) extends Job(args) {
  (TPipe.from[(Int,Int)](Tsv("in0",(0,1)), (0,1))
    join TPipe.from[(Int,Int)](Tsv("in1", (0,1)), (0,1)))
    .size
    .toPipe('key, 'count)
    .write(Tsv("out"))
}

class TPipeJoinCountTest extends Specification {
  import Dsl._
  "A TJoinCountJob" should {
    JobTest("com.twitter.scalding.TJoinCountJob")
      .source(Tsv("in0",(0,1)), List((0,1),(0,2),(1,1),(1,5)))
      .source(Tsv("in1",(0,1)), List((0,10),(1,20),(1,10),(1,30)))
      .sink[(Int,Long)](Tsv("out")) { outbuf =>
        val outMap = outbuf.toMap
        "correctly reduce after cogroup" in {
          outMap(0) must be_==(2)
          outMap(1) must be_==(6)
          outMap.size must be_==(2)
        }
      }
      .run
      .runHadoop
      .finish
  }
}

class TCrossJob(args : Args) extends Job(args) {
  (TextLine("in0") cross TextLine("in1"))
    .write(('left, 'right), Tsv("crossed"))
}

class TPipeCrossTest extends Specification {
  import Dsl._
  "A TCrossJob" should {
    TUtil.printStack {
    JobTest("com.twitter.scalding.TCrossJob")
      .source(TextLine("in0"), List((0,"you"),(1,"all")))
      .source(TextLine("in1"), List((0,"every"),(1,"body")))
      .sink[(String,String)](Tsv("crossed")) { outbuf =>
        val sortedL = outbuf.toList.sorted
        "create a cross-product" in {
          sortedL must be_==(List(("all","body"),
            ("all","every"),
            ("you","body"),
            ("you","every")))
        }
      }
      .run
      .runHadoop
      .finish
    }
  }
}
