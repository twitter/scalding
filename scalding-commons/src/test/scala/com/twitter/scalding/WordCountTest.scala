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

import org.scalatest.{ Matchers, WordSpec }

class WordCountTest extends WordSpec with Matchers {
  "A WordCount job" should {
    JobTest(new com.twitter.scalding.examples.WordCountJob(_))
      .arg("input", "inputFile")
      .arg("output", "outputFile")
      .source(TextLine("inputFile"), List((0, "hack hack hack and hack")))
      .sink[(String, Int)](TypedTsv[(String, Long)]("outputFile")){ outputBuffer =>
        val outMap = outputBuffer.toMap
        "count words correctly" in {
          outMap("hack") shouldBe 4
          outMap("and") shouldBe 1
        }
      }
      .run
      .finish()
  }
}
