package com.twitter.scalding

import cascading.tap.Tap
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

      def createIllegalTap(accessMode: AccessMode): Tap[Any, Any, Any] =
        testTapFactory.createTap(accessMode)(testMode).asInstanceOf[Tap[Any, Any, Any]]

      the[IllegalArgumentException] thrownBy {
        createIllegalTap(Read)
      } should have message ("requirement failed: " + TestTapFactory.sourceNotFoundError.format(testSource))

      the[IllegalArgumentException] thrownBy {
        createIllegalTap(Write)
      } should have message ("requirement failed: " + TestTapFactory.sinkNotFoundError.format(testSource))
    }
  }
}
