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

import cascading.tuple.Fields
import org.specs._
import scala.collection.JavaConverters._
import org.apache.hadoop.mapred.JobConf

class ReplTest extends Specification {
  import ReplImplicits._
  import ReplImplicitContext._

  val tutorialData = "../tutorial/data"
  val helloPath = tutorialData + "/hello.txt"

  def test() = {

    val suffix = mode match {
      case _: CascadingLocal => "local"
      case _: HadoopMode => "hadoop"
    }
    val testPath = "/tmp/scalding-repl/test/" + suffix + "/"
    val helloRef = List("Hello world", "Goodbye world")

    "save -- TypedPipe[String]" in {
      val hello = TypedPipe.from(TextLine(helloPath))
      val out = TypedTsv[String](testPath + "output0.txt")
      hello.save(out)

      val output = out.toIterator.toList
      output must_== helloRef
    }

    "snapshot" in {

      "only -- TypedPipe[String]" in {
        val hello = TypedPipe.from(TextLine(helloPath))
        val s: TypedPipe[String] = hello.snapshot
        // shallow verification that the snapshot was created correctly without
        // actually running a new flow to check the contents (just check that
        // it's a TypedPipe from a MemorySink or SequenceFile)
        s.toString must beMatching("IterablePipe|TypedPipeFactory")

        val pipeName = mode match {
          case m: HadoopMode => m.jobConf.get("hadoop.tmp.dir")
          case _ => "IterableSource"
        }
        s.toPipe(Fields.ALL).toString must beMatching(pipeName)

      }

      "can be mapped and saved -- TypedPipe[String]" in {
        val s = TypedPipe.from(TextLine(helloPath))
          .flatMap(_.split("\\s+"))
          .snapshot

        val out = TypedTsv[String](testPath + "output1.txt")

        // can call 'map' and 'save' on snapshot
        s.map(_.toLowerCase).save(out)

        val output = out.toIterator.toList
        output must_== helloRef.flatMap(_.split("\\s+")).map(_.toLowerCase)
      }

      "tuples -- TypedPipe[(String,Int)]" in {
        val s = TypedPipe.from(TextLine(helloPath))
          .flatMap(_.split("\\s+"))
          .map(w => (w.toLowerCase, w.length))
          .snapshot

        val output = s.toList
        output must_== helloRef.flatMap(_.split("\\s+")).map(w => (w.toLowerCase, w.length))
      }

      "grouped -- Grouped[String,String]" in {
        val grp = TypedPipe.from(TextLine(helloPath))
          .groupBy(_.toLowerCase)

        val correct = helloRef.map(l => (l.toLowerCase, l))

        "explicit" in {
          grp.snapshot.toList must_== correct
        }

        // Note: Must explicitly to toIterator because `grp.toList` resolves to `KeyedList.toList`
        "implicit" in {
          grp.toIterator.toList must_== correct
        }
      }

      "joined -- CoGrouped[String, Long]" in {
        val linesByWord = TypedPipe.from(TextLine(helloPath))
          .flatMap(_.split("\\s+"))
          .groupBy(_.toLowerCase)
        val wordScores = TypedPipe.from(TypedTsv[(String, Double)](tutorialData + "/word_scores.tsv")).group

        val grp = linesByWord.join(wordScores)
          .mapValues { case (text, score) => score }
          .sum

        val correct = Map("hello" -> 1.0, "goodbye" -> 3.0, "world" -> 4.0)

        "explicit" in {
          val s = grp.snapshot
          s.toIterator.toMap must_== correct
        }
        "implicit" in {
          grp.toIterator.toMap must_== correct
        }
      }

      "support toOption on ValuePipe" in {
        val hello = TypedPipe.from(TextLine(helloPath))
        val res = hello.map(_.length).sum
        val correct = helloRef.map(_.length).sum
        res.toOption must_== Some(correct)
      }
    }

    "reset flow" in {
      resetFlowDef()
      flowDef.getSources.asScala must beEmpty
    }

    "run entire flow" in {
      resetFlowDef()
      val hello = TypedPipe.from(TextLine(helloPath))
        .flatMap(_.split("\\s+"))
        .map(_.toLowerCase)
        .distinct

      val out = TypedTsv[String](testPath + "words.tsv")

      hello.write(out)
      run

      val words = out.toIterator.toSet
      words must_== Set("hello", "world", "goodbye")
    }

    "TypedPipe of a TextLine supports" in {
      val hello = TypedPipe.from(TextLine(helloPath))
      "toIterator" in {
        hello.toIterator.foreach { line: String =>
          line must beMatching("Hello world|Goodbye world")
        }
      }
      "toList" in {
        hello.toList must_== helloRef
      }
    }

    "toIterator should generate a snapshot for" in {
      val hello = TypedPipe.from(TextLine(helloPath))
      "TypedPipe with flatMap" in {
        val out = hello.flatMap(_.split("\\s+")).toList
        out must_== helloRef.flatMap(_.split("\\s+"))
      }
      "TypedPipe with tuple" in {
        hello.map(l => (l, l.length)).toList must_== helloRef.map(l => (l, l.length))
      }
    }

  }

  sequential

  "REPL in Local mode" should {
    ReplImplicits.mode = Local(strictSources = true)
    test()
  }

  "REPL in Hadoop mode" should {
    ReplImplicits.mode = Hdfs(strict = true, new JobConf)
    test()
  }
}
