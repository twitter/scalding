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

  def test() {
    val hello = TypedPipe.from(TextLine("tutorial/data/hello.txt"))

    val wordScores =
      TypedPipe.from(OffsetTextLine("tutorial/data/words.txt"))
        .map{ case (offset, word) => (word, offset) }
        .group

    // snapshot intermediate results without wiring everything up
    val s1 = hello.snapshot

    val s2 = hello.save(TypedTsv("dump.tsv"))

    // use snapshot in further flows
    val linesByWord = s1.flatMap(_.split("\\s+")).groupBy(_.toLowerCase)
    val counts = linesByWord.size

    // ensure snapshot enrichment works on KeyedListLike (CoGrouped, UnsortedGrouped), too
    val s3 = counts.snapshot

    val joined = linesByWord.join(wordScores)
    val s4 = joined.snapshot

    joined.write(TypedTsv("final_out.tsv"))
    // run the overall flow (with the 'final_out' sink), uses snapshot 's1'
    run
  }

}
