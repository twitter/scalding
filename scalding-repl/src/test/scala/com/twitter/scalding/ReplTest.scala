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

import java.io.File
import java.nio.file.Files
import org.scalatest.WordSpec
import scala.collection.JavaConverters._
import org.apache.hadoop.mapred.JobConf

class ReplTest extends WordSpec {
  import ReplImplicits._
  import ReplImplicitContext._

  val state = implicitly[BaseReplState]
  import state._

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
      assert(output === helloRef)
    }

    "snapshot" should {
      "only -- TypedPipe[String]" in {
        val hello = TypedPipe.from(TextLine(helloPath))
        val s: TypedPipe[String] = hello.snapshot
        // shallow verification that the snapshot was created correctly without
        // actually running a new flow to check the contents (just check that
        // it's a TypedPipe from a MemorySink or SequenceFile)
        s match {
          case TypedPipe.IterablePipe(_) => succeed
          case TypedPipe.SourcePipe(s) => assert(s.toString.contains("SequenceFile"))
          case _ => fail(s"expected an IterablePipe or source from a SequenceFile, found: $s")
        }
      }

      "can be mapped and saved -- TypedPipe[String]" in {
        val s = TypedPipe.from(TextLine(helloPath))
          .flatMap(_.split("\\s+"))
          .snapshot

        val out = TypedTsv[String](testPath + "output1.txt")

        // can call 'map' and 'save' on snapshot
        s.map(_.toLowerCase).save(out)

        val output = out.toIterator.toList
        assert(output === helloRef.flatMap(_.split("\\s+")).map(_.toLowerCase))
      }

      "tuples -- TypedPipe[(String,Int)]" in {
        val s = TypedPipe.from(TextLine(helloPath))
          .flatMap(_.split("\\s+"))
          .map(w => (w.toLowerCase, w.length))
          .snapshot

        val output = s.toList
        assert(output === helloRef.flatMap(_.split("\\s+")).map(w => (w.toLowerCase, w.length)))
      }

      "grouped -- Grouped[String,String]" which {
        val grp = TypedPipe.from(TextLine(helloPath))
          .groupBy(_.toLowerCase)

        val correct = helloRef.map(l => (l.toLowerCase, l))

        "is explicit" in {
          (grp.snapshot.toList === correct)
        }

        // Note: Must explicitly to toIterator because `grp.toList` resolves to `KeyedList.toList`
        "is implicit" in {
          assert(grp.toIterator.toList === correct)
        }
      }

      "joined -- CoGrouped[String, Long]" which {
        val linesByWord = TypedPipe.from(TextLine(helloPath))
          .flatMap(_.split("\\s+"))
          .groupBy(_.toLowerCase)
        val wordScores = TypedPipe.from(TypedTsv[(String, Double)](tutorialData + "/word_scores.tsv")).group

        val grp = linesByWord.join(wordScores)
          .mapValues { case (text, score) => score }
          .sum

        val correct = Map("hello" -> 1.0, "goodbye" -> 3.0, "world" -> 4.0)

        "is explicit" in {
          val s = grp.snapshot
          assert(s.toIterator.toMap === correct)
        }
        "is implicit" in {
          assert(grp.toIterator.toMap === correct)
        }
      }

      "support toOption on ValuePipe" in {
        val hello = TypedPipe.from(TextLine(helloPath))
        val res = hello.map(_.length).sum
        val correct = helloRef.map(_.length).sum
        assert(res.toOption === Some(correct))
      }
    }

    "reset flow" in {
      resetFlowDef()
      assert(flowDef.getSources.asScala.isEmpty)
    }

    "run entire flow" in {
      resetFlowDef()
      val hello = TypedPipe.from(TextLine(helloPath))
        .flatMap(_.split("\\s+"))
        .map(_.toLowerCase)
        .distinct

      val out = TypedTsv[String](testPath + "words.tsv")

      hello.write(out)
      ReplState.run

      val words = out.toIterator.toSet
      assert(words === Set("hello", "world", "goodbye"))
    }

    "TypedPipe of a TextLine" should {
      val hello = TypedPipe.from(TextLine(helloPath))
      "support toIterator" in {
        hello.toIterator.foreach { line: String =>
          assert(line.contains("Hello world") || line.contains("Goodbye world"))
        }
      }
      "support toList" in {
        assert(hello.toList === helloRef)
      }
    }

    "toIterator should generate a snapshot for TypedPipe with" should {
      val hello = TypedPipe.from(TextLine(helloPath))
      "flatMap" in {
        val out = hello.flatMap(_.split("\\s+")).toList
        assert(out === helloRef.flatMap(_.split("\\s+")))
      }
      "tuple" in {
        assert(hello.map(l => (l, l.length)).toList === helloRef.map(l => (l, l.length)))
      }
    }
  }

  "REPL in Local mode" should {
    mode = Local(strictSources = true)
    test()
  }

  "REPL in Hadoop mode" should {
    mode = Hdfs(strict = true, new JobConf)
    test()
  }

  "findAllUpPath" should {
    val root = Files.createTempDirectory("scalding-repl").toFile
    root.deleteOnExit()

    val matchingFile = new File(root, "this_matches")
    matchingFile.createNewFile()

    val currentDirectory = new File(root, "arbitrary_directory")
    currentDirectory.mkdir()

    "enumerate matching files" in {
      root.setReadable(true)

      val actual = ScaldingILoop
        .findAllUpPath(currentDirectory.getAbsolutePath)("this_matches")
      assert(actual === List(matchingFile))
    }

    "ignore directories with restricted permissions" in {
      root.setReadable(false)

      val actual = ScaldingILoop
        .findAllUpPath(currentDirectory.getAbsolutePath)("this_matches")
      assert(actual === List.empty)
    }
  }
}
