package com.twitter.scalding

import org.specs._
import com.twitter.scalding._
import com.codahale.jerkson.Json

class JsonLineJob(args : Args) extends Job(args) {
  try {
    Tsv("input0", ('query, 'queryStats)).read.write(JsonLine("output0"))
  } catch {
    case e : Exception => e.printStackTrace()
  }
}

class FileSourceTest extends Specification {
  noDetailedDiffs()
  import Dsl._

  "A JsonLine sink" should {
    JobTest("com.twitter.scalding.JsonLineJob")
      .source(Tsv("input0", ('query, 'queryStats)), List(("doctor's mask", List(42.1f, 17.1f))))
      .sink[String](JsonLine("output0")) { buf =>
        val json = buf.head
        "not stringify lists or numbers and not escape single quotes" in {
            json must be_==("""{"query":"doctor's mask","queryStats":[42.1,17.1]}""")
        }
      }
      .run
      .finish
  }
}