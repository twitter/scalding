package com.twitter.scalding

import java.util.UUID

import org.scalacheck.Prop._
import org.scalacheck.{ Gen, Properties }

object RichPipeSpecification extends Properties("RichPipe") {

  import Gen._
  import cascading.pipe.{ Pipe => CPipe }

  def extractPipeNumber(pipeName: String) = pipeName match {
    case RichPipe.FormerAssignedPipeNamePattern(pipenum) => pipenum.toInt
    case _ => 0
  }

  /* Note: in these tests, we can never compare to equality with the basePipeNumber or offsets from that; as the pipe
  assigned names number sequence is a global atomic integer, and the test framework might run other tests in parallel
  to this, we can only count on it being monotonically increasing. */

  property("assignName carries over the old number " +
    "if it was already an assigned name") = forAll(posNum[Int]) { (oldNum: Int) =>
    val basePipeNumber = extractPipeNumber(RichPipe.getNextName)

    val p = new CPipe(s"_pipe_${oldNum}")
    val ap = RichPipe.assignName(p)

    val newNum = extractPipeNumber(ap.getName)

    (newNum > basePipeNumber) && ap.getName.endsWith(s"-${oldNum}")
  }

  property("assignName carries over the last (12-hexdigits) group from the UUID " +
    "if the old name included one") = forAll(alphaStr, uuid, alphaStr) {
    (prefix: String, uuid: UUID, suffix: String) =>
      val basePipeNumber = extractPipeNumber(RichPipe.getNextName)

      val lastGroup = uuid.toString.split("-").last
      val p = new CPipe(prefix + uuid + suffix)
      val ap = RichPipe.assignName(p)

      val newNum = extractPipeNumber(ap.getName)

      (newNum > basePipeNumber) && ap.getName.endsWith(s"-${lastGroup}")
  }

  property("assignName carries over the last (12-hexdigits) group from the *last* UUID " +
    "if the old name included more than one") = forAll(alphaStr, uuid, alphaStr, uuid, alphaStr) {
    (prefix: String, uuid1: UUID, middle: String, uuid: UUID, suffix: String) =>
      val basePipeNumber = extractPipeNumber(RichPipe.getNextName)

      val lastGroup = uuid.toString.split("-").last
      val p = new CPipe(prefix + uuid1 + middle + uuid + suffix)
      val ap = RichPipe.assignName(p)

      val newNum = extractPipeNumber(ap.getName)

      (newNum > basePipeNumber) && ap.getName.endsWith(s"-${lastGroup}")
  }

  property("assignName carries over the over the old number "
    + "if it was already an assigned name carrying bits from a UUID") = forAll(posNum[Int], uuid) {
    (oldNum: Int, uuid: UUID) =>
      val basePipeNumber = extractPipeNumber(RichPipe.getNextName)
      val lastGroup = uuid.toString.split("-").last

      val p = new CPipe(s"_pipe_${oldNum}-${lastGroup}")
      val ap = RichPipe.assignName(p)

      val newNum = extractPipeNumber(ap.getName)

      (newNum > basePipeNumber) && ap.getName.endsWith(s"-${oldNum}")
  }

  val smallNames = Gen.choose(0, 12) flatMap { sz => Gen.listOfN(sz, alphaChar) } map (_.mkString)

  val longNames = Gen.choose(13, 256) flatMap { sz => Gen.listOfN(sz, alphaChar) } map (_.mkString)

  property("assignName carries over the whole old name "
    + "if it's 12 characters or less") = forAll(smallNames) {
    (name: String) =>
      val basePipeNumber = extractPipeNumber(RichPipe.getNextName)
      val p = new CPipe(name)
      val ap = RichPipe.assignName(p)

      val newNum = extractPipeNumber(ap.getName)

      (newNum > basePipeNumber) && ap.getName.endsWith(s"-${name}")
  }

  property("assignName carries over the last 12 characters of the old name "
    + "if it's more than 12 characters") = forAll(longNames) {
    (name: String) =>
      val basePipeNumber = extractPipeNumber(RichPipe.getNextName)
      val nameEnd = name.subSequence(name.length - 12, name.length)
      val p = new CPipe(name)
      val ap = RichPipe.assignName(p)

      val newNum = extractPipeNumber(ap.getName)

      (newNum > basePipeNumber) && ap.getName.endsWith(s"-${nameEnd}")
  }

}