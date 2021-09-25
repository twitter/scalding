package com.twitter.scalding.platform

import com.twitter.scalding._

/*
 * These jobs are used in PlatformTests that test correct line numbers in descriptions.
 * Placing them in a separate file means we don't have to update the tests that care about
 * line numbers when PlatformTest.scala changes for unrelated reasons.
 */

class TypedPipeJoinWithDescriptionJob(args: Args) extends Job(args) {
  PlatformTest.setAutoForceRight(mode, true)

  val x = TypedPipe.from[(Int, Int)](List((1, 1)))
  val y = TypedPipe.from[(Int, String)](List((1, "first")))
  val z = TypedPipe.from[(Int, Boolean)](List((2, true))).group

  x.hashJoin(y) // this triggers an implicit that somehow pushes the line number to the next one
    .withDescription("hashJoin")
    .leftJoin(z)
    .withDescription("leftJoin")
    .values
    .write(TypedTsv[((Int, String), Option[Boolean])]("output"))
}

class TypedPipeWithDescriptionJob(args: Args) extends Job(args) {
  TypedPipe.from[String](List("word1", "word1", "word2"))
    .withDescription("map stage - assign words to 1")
    .map { w => (w, 1L) }
    .group
    .withDescription("reduce stage - sum")
    .sum
    .withDescription("write")
    .write(TypedTsv[(String, Long)]("output"))
}
