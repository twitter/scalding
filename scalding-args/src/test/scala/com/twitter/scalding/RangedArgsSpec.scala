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

import org.specs._

class RangeSpecs extends Specification {
  "A Range" should {
    val testRange = Range(4, 5)

    "contain its endpoints" in {
      testRange.lower must_== 4
      testRange.upper must_== 5
    }

    "throw errors for misordered ranges" in {
      Range(4, 4)
      Range(5, 4) must throwAn[AssertionError]
    }

    "assert lower bounds" in {
      testRange.assertLowerBound(3)
      testRange.assertLowerBound(4)
      testRange.assertLowerBound(5) must throwAn[AssertionError]
    }

    "assert upper bounds" in {
      testRange.assertUpperBound(6)
      testRange.assertUpperBound(5)
      testRange.assertUpperBound(4) must throwAn[AssertionError]
    }

    "print nicely with mkString" in {
      "for trivial ranges" in {
        Range(4, 4).mkString("_") must beEqualTo("4")
      }
      "for proper ranges" in {
        testRange.mkString("_") must beEqualTo("4_5")
        testRange.mkString("-") must beEqualTo("4-5")
      }
    }
  }
}
