package com.twitter.scalding.examples

import com.twitter.scalding._

class WordCountJob(args: Args) extends Job(args) {
  TypedPipe.from(TextLine(args("input")))
    .flatMap { line: String => line.split("\\s+") }
    .map (_ -> 1L)
    .sumByKey
    .write(TypedTsv[(String, Long)](args("output")))
}
