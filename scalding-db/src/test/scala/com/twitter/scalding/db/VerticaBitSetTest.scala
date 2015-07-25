/*
Copyright 2015 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
