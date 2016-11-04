package com.twitter.scalding

import cascading.tap.{ SinkMode, Tap }
import cascading.tuple.{ Fields, Tuple }

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
        testMode.storageMode.createTap(testSource, Read, testMode, SinkMode.UPDATE).asInstanceOf[Tap[Any, Any, Any]]

      the[IllegalArgumentException] thrownBy {
        createIllegalTap()
      } should have message ("Failed to create tap for: com.twitter.scalding.Tsv(path), with error: requirement failed: " + TestTapFactory.sourceNotFoundError.format(testSource))
    }
  }
}
