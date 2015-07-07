package com.twitter.scalding.db

import org.scalatest.WordSpec
import scala.util.Random

class VerticaBitSetTest extends WordSpec {
  "Vertica BitSet" should {
    "Should do the right fields" in {

      def testCase = (0 until Random.nextInt(11)).map { _ =>
        Random.nextInt(11)
      }.toSet

      (0 until 10000).foreach { _ =>
        val tc = testCase
        val bitSet = VerticaBitSet(11)

        tc.foreach { e =>
          bitSet.setNull(e)
        }

        (0 until 11).foreach{ idx =>
          assert(bitSet.isSet(idx) == tc.contains(idx), s"Index $idx is in set ${tc} but not showing as set in bitset: $bitSet")
        }
      }
    }
  }
}
