package com.twitter.scalding

import org.specs._
import com.twitter.scalding._
import com.codahale.jerkson.Json
import org.apache.hadoop.io.Text

class JsonLineJob(args : Args) extends Job(args) {
  try {
    Tsv("input0", ('query, 'queryStats)).read.write(JsonLine("output0"))
  } catch {
    case e : Exception => e.printStackTrace()
  }
}

class JsonLineRestrictedFieldsJob(args : Args) extends Job(args) {
  try {
    Tsv("input0", ('query, 'queryStats)).read.write(JsonLine("output0", Tuple1('query)))
  } catch {
    case e : Exception => e.printStackTrace()
  }
}

class JsonLineInputJob(args : Args) extends Job(args) {
  try {

    JsonLine("input0", ('foo, 'bar)).read
      .project('foo, 'bar)
      .write(Tsv("output0"))

  } catch { 
    case e : Exception => e.printStackTrace
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

    JobTest("com.twitter.scalding.JsonLineRestrictedFieldsJob")
      .source(Tsv("input0", ('query, 'queryStats)), List(("doctor's mask", List(42.1f, 17.1f))))
      .sink[String](JsonLine("output0", Tuple1('query))) { buf =>
        val json = buf.head
        "only sink requested fields" in {
            json must be_==("""{"query":"doctor's mask"}""")
        }
      }
      .run
      .finish

    val json = """{"foo": 3, "bar": "baz"}\n"""

    JobTest("com.twitter.scalding.JsonLineInputJob")
      .source(JsonLine("input0", ('foo, 'bar)), List((0, json)))
      .sink[(Int, String)](Tsv("output0")) {
        outBuf => 
          "read json line input" in {
            outBuf.toList must be_==(List((3, "baz")))
          }
      }
      .run
      .finish 

    val json2 = """{"foo": 7 }\n"""

    JobTest("com.twitter.scalding.JsonLineInputJob")
      .source(JsonLine("input0", ('foo, 'bar)), List((0, json), (1, json2)))
      .sink[(Int, String)](Tsv("output0")) {
        outBuf => 
          "handle missing fields" in {
            outBuf.toList must be_==(List((3, "baz"), (7, null)))
          }
      }
      .run
      .finish 

  }
}

  class WritableSequenceFileJob(args : Args) extends Job(args) {
    import TDsl._
  WritableSequenceFile[Text,Text](args("input"))
    .flatMap { case(offset,line) => line.toString.split("\\s+") }
    .map {(_,1L)}
    .group
    .sum
    .write( TypedTsv[(String,Long)]( args("output") ) )
}


  class WritableSequenceFileTest extends Specification with TupleConversions {
  "A WritableSequenceFileJob job" should {
    JobTest("com.twitter.scalding.WritableSequenceFileJob").
      arg("input", "inputFile").
      arg("output", "outputFile").
      source(WritableSequenceFile[Text,Text]("inputFile"), List(new Text("0") -> new Text("hack hack hack and hack"))).
      sink[(String,Long)](TypedTsv[(String,Long)]("outputFile")){ outputBuffer =>
        val outMap = outputBuffer.toMap
        "load Text sequence files and count words correctly" in {
          outMap("hack") must be_==(4)
          outMap("and") must be_==(1)
        }
      }.
      runHadoop.
      finish
  }
}


