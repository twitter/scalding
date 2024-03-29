package com.twitter.scalding

import org.scalatest.{Matchers, WordSpec}

class TypedSketchJoinJobForEmptyKeys(args: Args) extends Job(args) {
  // Deal with when a key appears in left but not right
  val leftTypedPipe = TypedPipe.from(List((1, 1111)))
  val rightTypedPipe = TypedPipe.from(List((3, 3333), (4, 4444)))

  implicit def serialize(k: Int): Array[Byte] = k.toString.getBytes

  val sketched = leftTypedPipe
    .sketch(1)
    .leftJoin(rightTypedPipe)

  // this is test that a TypedPipe.Keyed method works:
  sketched.values

  sketched
    .map { case (a, (b, c)) =>
      (a, b, c.getOrElse(-1))
    }
    .write(TypedTsv("output"))
}

class TypedSketchJoinJobForEmptyKeysTest extends WordSpec with Matchers {
  "A TypedSketchJoinJobForEmptyKeysTest" should {
    "Sketch leftJoin with a single left key should be correct" in {
      JobTest(new TypedSketchJoinJobForEmptyKeys(_))
        .sink[(Int, Int, Int)](TypedTsv[(Int, Int, Int)]("output")) { outBuf =>
          outBuf should have size 1
          val unordered = outBuf.toSet
          unordered should contain(1, 1111, -1)
        }
        .run
        .finish()
    }
  }
}
