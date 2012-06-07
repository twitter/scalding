package com.twitter.scalding

import org.specs._

import TDsl._

class TPipeJob(args : Args) extends Job(args) {
  //Word count using TPipe
  TPipe.from(TextLine("inputFile"))
    .flatMap { _.split("\\s+") }
    .map { w => (w, 1L) }
    .sum
    .toPipe('key, 'value)
    .write(Tsv("outputFile"))
}

class TPipeTest extends Specification with TupleConversions {
  "A TPipe" should {
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

class TPipeJoinJob(args : Args) extends Job(args) {
  (TPipe.from[(Int,Int)](Tsv("inputFile0").read, (0, 1))
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
