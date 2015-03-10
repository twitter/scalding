package com.twitter.scalding.examples

import com.twitter.scalding._

class WordCountJob(args: Args) extends Job(args) {
  TypedPipe.from(TextLine(args("input")))
    .flatMap { line => line.split("\\s+") }
    .map { word => (word, 1L) }
    .sumByKey
    // The compiler will enforce the type coming out of the sumByKey is the same as the type we have for our sink
    .write(TypedTsv[(String, Long)](args("output")))
}
