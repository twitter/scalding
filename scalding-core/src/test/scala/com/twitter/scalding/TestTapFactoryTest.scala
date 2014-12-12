package com.twitter.scalding

import cascading.tap.Tap
import cascading.tuple.{ Fields, Tuple }
import java.lang.IllegalArgumentException
import scala.collection.mutable.Buffer
import org.scalatest.{ Matchers, WordSpec }

class TestTapFactoryTest extends WordSpec with Matchers {
  "A test tap created by TestTapFactory" should {
    "error helpfully when a source is not in the map for test buffers" in {
      // Source to use for this test.
      val testSource = Tsv("path")

      // Map of sources to use when creating the tap-- does not contain testSource
      val emptySourceMap = Map[Source, Buffer[Tuple]]()

      val testMode = Test { emptySourceMap.get(_) }
      val testTapFactory = TestTapFactory(testSource, new Fields())

      def createIllegalTap(): Tap[Any, Any, Any] =
        testTapFactory.createTap(Read)(testMode).asInstanceOf[Tap[Any, Any, Any]]

      the[IllegalArgumentException] thrownBy {
        createIllegalTap()
      } should have message ("requirement failed: " + TestTapFactory.sourceNotFoundError.format(testSource))
    }
  }
}
