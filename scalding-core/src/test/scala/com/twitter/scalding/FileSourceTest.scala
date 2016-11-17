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

import cascading.scheme.NullScheme
import cascading.tuple.Fields
import org.apache.hadoop.conf.Configuration
import org.scalatest.{ Matchers, WordSpec }

class MultiTsvInputJob(args: Args) extends Job(args) {
  try {
    MultipleTsvFiles(List("input0", "input1"), ('query, 'queryStats)).read.write(Tsv("output0"))
  } catch {
    case e: Exception => e.printStackTrace()
  }

}

class SequenceFileInputJob(args: Args) extends Job(args) {
  try {
    SequenceFile("input0").read.write(SequenceFile("output0"))
    WritableSequenceFile("input1", ('query, 'queryStats)).read.write(WritableSequenceFile("output1", ('query, 'queryStats)))
  } catch {
    case e: Exception => e.printStackTrace()
  }
}

class MultipleTextLineFilesJob(args: Args) extends Job(args) {
  try {
    MultipleTextLineFiles(args.list("input"): _*).write(Tsv("output0"))
  } catch {
    case e: Exception => e.printStackTrace()
  }

}

class FileSourceTest extends WordSpec with Matchers {
  import Dsl._

  "A MultipleTsvFile Source" should {
    JobTest(new MultiTsvInputJob(_)).
      source(MultipleTsvFiles(List("input0", "input1"), ('query, 'queryStats)),
        List(("foobar", 1), ("helloworld", 2))).
        sink[(String, Int)](Tsv("output0")) {
          outBuf =>
            "take multiple Tsv files as input sources" in {
              outBuf should have length 2
              outBuf.toList shouldBe List(("foobar", 1), ("helloworld", 2))
            }
        }
      .run
      .finish()
  }

  "A WritableSequenceFile Source" should {
    JobTest(new SequenceFileInputJob(_)).
      source(SequenceFile("input0"),
        List(("foobar0", 1), ("helloworld0", 2))).
        source(WritableSequenceFile("input1", ('query, 'queryStats)),
          List(("foobar1", 1), ("helloworld1", 2))).
          sink[(String, Int)](SequenceFile("output0")) {
            outBuf =>
              "sequence file input" in {
                outBuf should have length 2
                outBuf.toList shouldBe List(("foobar0", 1), ("helloworld0", 2))
              }
          }
      .sink[(String, Int)](WritableSequenceFile("output1", ('query, 'queryStats))) {
        outBuf =>
          "writable sequence file input" in {
            outBuf should have length 2
            outBuf.toList shouldBe List(("foobar1", 1), ("helloworld1", 2))
          }
      }
      .run
      .finish()
  }

  "A MultipleTextLineFiles Source" should {
    JobTest(new MultipleTextLineFilesJob(_))
      .arg("input", List("input0", "input1"))
      .source(MultipleTextLineFiles("input0", "input1"), List("foobar", "helloworld"))
      .sink[String](Tsv("output0")) { outBuf =>
        "take multiple text files as input sources" in {
          outBuf should have length 2
          outBuf.toList shouldBe List("foobar", "helloworld")
        }
      }
      .run
      .finish()
  }

  "TextLine.toIterator" should {
    "correctly read strings" in {
      TextLine("../tutorial/data/hello.txt").toIterator(Config.default, Local(true)).toList shouldBe List("Hello world", "Goodbye world")
    }
  }

  /**
   * The layout of the test data looks like this:
   * /test_data/2013/02 does not exist
   *
   * /test_data/2013/03                 (dir with a single data file in it)
   * /test_data/2013/03/2013-03.txt
   *
   * /test_data/2013/04                 (dir with a single data file and a _SUCCESS file)
   * /test_data/2013/04/2013-04.txt
   * /test_data/2013/04/_SUCCESS
   *
   * /test_data/2013/05                 (logically empty dir: git does not support empty dirs)
   *
   * /test_data/2013/06                 (dir with only a _SUCCESS file)
   * /test_data/2013/06/_SUCCESS
   *
   * /test_data/2013/07
   * /test_data/2013/07/2013-07.txt
   * /test_data/2013/07/_SUCCESS
   */
  "default pathIsGood" should {
    import TestFileSource.pathIsGood
    "reject a non-existing directory" in {
      pathIsGood("test_data/2013/02/") shouldBe false
      pathIsGood("test_data/2013/02/*") shouldBe false
    }

    "accept a directory with data in it" in {
      pathIsGood("test_data/2013/03/") shouldBe true
      pathIsGood("test_data/2013/03/*") shouldBe true
    }

    "accept a directory with data and _SUCCESS in it" in {
      pathIsGood("test_data/2013/04/") shouldBe true
      pathIsGood("test_data/2013/04/*") shouldBe true
    }

    "accept a single directory without glob" in {
      pathIsGood("test_data/2013/05/") shouldBe true
    }

    "reject a single directory glob with ignored files" in {
      pathIsGood("test_data/2013/05/*") shouldBe false
    }

    "reject a directory with only _SUCCESS when specified as a glob" in {
      pathIsGood("test_data/2013/06/*") shouldBe false
    }

    "accept a directory with only _SUCCESS when specified without a glob" in {
      pathIsGood("test_data/2013/06/") shouldBe true
    }
  }

  "FileSource.globHasSuccessFile" should {
    import TestFileSource.globHasSuccessFile

    "accept a directory glob with only _SUCCESS" in {
      globHasSuccessFile("test_data/2013/06/*") shouldBe true
    }

    "accept a directory glob with _SUCCESS and other hidden files" in {
      globHasSuccessFile("test_data/2013/05/*") shouldBe true
    }

    "accept a directory glob with _SUCCESS and other non-hidden files" in {
      globHasSuccessFile("test_data/2013/04/*") shouldBe true
    }

    "reject a path without glob" in {
      globHasSuccessFile("test_data/2013/04/") shouldBe false
    }

    "reject a multi-dir glob without _SUCCESS" in {
      globHasSuccessFile("test_data/2013/{02,03}/*") shouldBe false
    }
  }

  "success file source pathIsGood" should {
    import TestSuccessFileSource.pathIsGood

    "reject a non-existing directory" in {
      pathIsGood("test_data/2013/02/") shouldBe false
      pathIsGood("test_data/2013/02/*") shouldBe false
    }

    "reject a directory with data in it but no _SUCCESS file" in {
      pathIsGood("test_data/2013/03/") shouldBe false
      pathIsGood("test_data/2013/03/*") shouldBe false
    }

    "reject a single directory without glob" in {
      pathIsGood("test_data/2013/05/") shouldBe false
    }

    "reject a single directory glob with only _SUCCESS and ignored files" in {
      pathIsGood("test_data/2013/05/*") shouldBe false
    }

    "accept a directory with data and _SUCCESS in it when specified as a glob" in {
      pathIsGood("test_data/2013/04/*") shouldBe true
    }

    "reject a directory with data and _SUCCESS in it when specified without a glob" in {
      pathIsGood("test_data/2013/04/") shouldBe false
    }

    "reject a directory with only _SUCCESS when specified as a glob" in {
      pathIsGood("test_data/2013/06/*") shouldBe false
    }

    "reject a directory with only _SUCCESS when specified without a glob" in {
      pathIsGood("test_data/2013/06/") shouldBe false
    }

    "reject a multi-dir glob with only one _SUCCESS" in {
      pathIsGood("test_data/2013/{03,04}/*") shouldBe false
    }

    "accept a multi-dir glob if every dir has _SUCCESS" in {
      pathIsGood("test_data/2013/{04,08}/*") shouldBe true
    }

    "accept a multi-dir glob if all dirs with non-hidden files have _SUCCESS while dirs with " +
      "hidden ones don't" in {
      pathIsGood("test_data/2013/{04,05}/*") shouldBe true
    }

    // NOTE: this is an undesirable limitation of SuccessFileSource, and is encoded here
    // as a demonstration. This isn't a great behavior that we'd want to keep.
    "accept a multi-dir glob if all dirs with non-hidden files have _SUCCESS while other dirs " +
      "are empty or don't exist" in {
        pathIsGood("test_data/2013/{02,04,05}/*") shouldBe true
      }
  }

  "FixedPathSource.hdfsWritePath" should {
    "crib if path == *" in {
      intercept[AssertionError] { TestFixedPathSource("*").hdfsWritePath }
    }

    "crib if path == /*" in {
      intercept[AssertionError] { TestFixedPathSource("/*").hdfsWritePath }
    }

    "remove /* from a path ending in /*" in {
      TestFixedPathSource("test_data/2013/06/*").hdfsWritePath shouldBe "test_data/2013/06"
    }

    "leave path as-is when it ends in a directory name" in {
      TestFixedPathSource("test_data/2013/06").hdfsWritePath shouldBe "test_data/2013/06"
    }

    "leave path as-is when it ends in a directory name/" in {
      TestFixedPathSource("test_data/2013/06/").hdfsWritePath shouldBe "test_data/2013/06/"
    }

    "leave path as-is when it ends in * without a preceeding /" in {
      TestFixedPathSource("test_data/2013/06*").hdfsWritePath shouldBe "test_data/2013/06*"
    }
  }

  "invalid source input" should {
    "Throw in validateTaps in strict mode" in {
      val e = intercept[InvalidSourceException] {
        TestInvalidFileSource.validateTaps(Hdfs(strict = true, new Configuration()))
      }
      assert(e.getMessage.endsWith("Data is missing from one or more paths in: List(invalid_hdfs_path)"))
    }

    "Throw in validateTaps in non-strict mode" in {
      val e = intercept[InvalidSourceException] {
        TestInvalidFileSource.validateTaps(Hdfs(strict = false, new Configuration()))
      }
      assert(e.getMessage.endsWith("No good paths in: List(invalid_hdfs_path)"))
    }

    "Throw in toIterator because no data is present in strict mode" in {
      val e = intercept[InvalidSourceException] {
        TestInvalidFileSource.toIterator(Config.default, Hdfs(strict = true, new Configuration()))
      }
      assert(e.getMessage.endsWith("Data is missing from one or more paths in: List(invalid_hdfs_path)"))
    }

    "Throw in toIterator because no data is present in non-strict mode" in {
      val e = intercept[InvalidSourceException] {
        TestInvalidFileSource.toIterator(Config.default, Hdfs(strict = false, new Configuration()))
      }
      assert(e.getMessage.endsWith("No good paths in: List(invalid_hdfs_path)"))
    }
  }
}

object TestPath {
  def getCurrentDirectory = new java.io.File(".").getCanonicalPath
  def prefix = getCurrentDirectory.split("/").last match {
    case "scalding-core" => getCurrentDirectory
    case _ => getCurrentDirectory + "/scalding-core"
  }
  val testfsPathRoot = prefix + "/src/test/resources/com/twitter/scalding/test_filesystem/"
}

object TestFileSource extends FileSource {
  import TestPath.testfsPathRoot

  override def hdfsPaths: Iterable[String] = Iterable.empty
  override def localPaths: Iterable[String] = Iterable.empty

  val conf = new Configuration()

  def pathIsGood(p: String) = super.pathIsGood(testfsPathRoot + p, conf)
  def globHasSuccessFile(p: String) = FileSource.globHasSuccessFile(testfsPathRoot + p, conf)
}

object TestSuccessFileSource extends FileSource with SuccessFileSource {
  import TestPath.testfsPathRoot
  override def hdfsPaths: Iterable[String] = Iterable.empty
  override def localPaths: Iterable[String] = Iterable.empty

  val conf = new Configuration()

  def pathIsGood(p: String) = super.pathIsGood(testfsPathRoot + p, conf)
}

object TestInvalidFileSource extends FileSource with Mappable[String] {
  override def hdfsPaths: Iterable[String] = Iterable("invalid_hdfs_path")
  override def localPaths: Iterable[String] = Iterable("invalid_local_path")
  override def hdfsScheme = new NullScheme(Fields.ALL, Fields.NONE)
  override def converter[U >: String] =
    TupleConverter.asSuperConverter[String, U](implicitly[TupleConverter[String]])
}

case class TestFixedPathSource(path: String*) extends FixedPathSource(path: _*)
