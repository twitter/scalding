package com.twitter.scalding

import org.specs._

import TPipe.pipeToGrouped

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
