/*
Copyright 2012 Twitter, Inc.

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

package com.twitter.scalding

import org.scalatest.WordSpec

class RangeSpecs extends WordSpec {
  "A Range" should {
    val testRange = Range(4, 5)

    "contain its endpoints" in {
      assert(testRange.lower === 4)
      assert(testRange.upper === 5)
    }

    "throw errors for misordered ranges" in {
      Range(4, 4)
      intercept[AssertionError] { Range(5, 4) }
    }

    "assert lower bounds" in {
      testRange.assertLowerBound(3)
      testRange.assertLowerBound(4)
      intercept[AssertionError] { testRange.assertLowerBound(5) }
    }

    "assert upper bounds" in {
      testRange.assertUpperBound(6)
      testRange.assertUpperBound(5)
      intercept[AssertionError] { testRange.assertUpperBound(4) }
    }

    "print nicely with mkString" should {
      "for trivial ranges" in {
        assert(Range(4, 4).mkString("_") === "4")
      }
      "for proper ranges" in {
        assert(testRange.mkString("_") === "4_5")
        assert(testRange.mkString("-") === "4-5")
      }
    }
  }
}
