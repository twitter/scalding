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

object ReplTest {
  import ReplImplicits._

  def compileCheck() {
    val hello = TypedPipe.from(TextLine("tutorial/data/hello.txt"))
    val s1 = hello.snapshot
    val s2 = s1.save(TypedTsv("dump.tsv"))

    val linesByWord = s2.flatMap(_.split("\\s+")).groupBy(_.toLowerCase)
    val counts = linesByWord.size

    val s3 = counts.snapshot

    val wordScores =
      TypedPipe.from(OffsetTextLine("tutorial/data/words.txt"))
        .map{ case (offset, word) => (word, offset) }
        .group

    val scoredLines = linesByWord.join(wordScores)
    val s4 = scoredLines.snapshot
  }

}
