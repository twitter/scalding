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

import org.specs._
import org.apache.hadoop.conf.Configuration

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

class FileSourceTest extends Specification {
  noDetailedDiffs()
  import Dsl._

  "A MultipleTsvFile Source" should {
    JobTest(new MultiTsvInputJob(_)).
      source(MultipleTsvFiles(List("input0", "input1"), ('query, 'queryStats)),
        List(("foobar", 1), ("helloworld", 2))).
        sink[(String, Int)](Tsv("output0")) {
          outBuf =>
            "take multiple Tsv files as input sources" in {
              outBuf.length must be_==(2)
              outBuf.toList must be_==(List(("foobar", 1), ("helloworld", 2)))
            }
        }
      .run
      .finish
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
                outBuf.length must be_==(2)
                outBuf.toList must be_==(List(("foobar0", 1), ("helloworld0", 2)))
              }
          }
      .sink[(String, Int)](WritableSequenceFile("output1", ('query, 'queryStats))) {
        outBuf =>
          "writable sequence file input" in {
            outBuf.length must be_==(2)
            outBuf.toList must be_==(List(("foobar1", 1), ("helloworld1", 2)))
          }
      }
      .run
      .finish
  }
  "TextLine.toIterator" should {
    "correctly read strings" in {
      TextLine("../tutorial/data/hello.txt").toIterator(Config.default, Local(true)).toList must be_==(
        List("Hello world", "Goodbye world"))
    }
  }

  /**
   * The layout of the test data looks like this:
   *
   * /test_data/2013/03                 (dir with a single data file in it)
   * /test_data/2013/03/2013-03.txt
   *
   * /test_data/2013/04                 (dir with a single data file and a _SUCCESS file)
   * /test_data/2013/04/2013-04.txt
   * /test_data/2013/04/_SUCCESS
   *
   * /test_data/2013/05                 (empty dir)
   *
   * /test_data/2013/06                 (dir with only a _SUCCESS file)
   * /test_data/2013/06/_SUCCESS
   */
  "default pathIsGood" should {
    import TestFileSource.pathIsGood

    "accept a directory with data in it" in {
      pathIsGood("test_data/2013/03/") must be_==(true)
      pathIsGood("test_data/2013/03/*") must be_==(true)
    }

    "accept a directory with data and _SUCCESS in it" in {
      pathIsGood("test_data/2013/04/") must be_==(true)
      pathIsGood("test_data/2013/04/*") must be_==(true)
    }

    "reject an empty directory" in {
      pathIsGood("test_data/2013/05/") must be_==(false)
      pathIsGood("test_data/2013/05/*") must be_==(false)
    }

    "reject a directory with only _SUCCESS when specified as a glob" in {
      pathIsGood("test_data/2013/06/*") must be_==(false)
    }

    "accept a directory with only _SUCCESS when specified without a glob" in {
      pathIsGood("test_data/2013/06/") must be_==(true)
    }
  }

  "success file source pathIsGood" should {
    import TestSuccessFileSource.pathIsGood

    "reject a directory with data in it but no _SUCCESS file" in {
      pathIsGood("test_data/2013/03/") must be_==(false)
      pathIsGood("test_data/2013/03/*") must be_==(false)
    }

    "accept a directory with data and _SUCCESS in it when specified as a glob" in {
      pathIsGood("test_data/2013/04/*") must be_==(true)
    }

    "reject a directory with data and _SUCCESS in it when specified without a glob" in {
      pathIsGood("test_data/2013/04/") must be_==(false)
    }

    "reject an empty directory" in {
      pathIsGood("test_data/2013/05/") must be_==(false)
      pathIsGood("test_data/2013/05/*") must be_==(false)
    }

    "reject a directory with only _SUCCESS when specified as a glob" in {
      pathIsGood("test_data/2013/06/*") must be_==(false)
    }

    "reject a directory with only _SUCCESS when specified without a glob" in {
      pathIsGood("test_data/2013/06/") must be_==(false)
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
  override def localPath: String = ""

  val conf = new Configuration()

  def pathIsGood(p: String) = super.pathIsGood(testfsPathRoot + p, conf)
}

object TestSuccessFileSource extends FileSource with SuccessFileSource {
  import TestPath.testfsPathRoot
  override def hdfsPaths: Iterable[String] = Iterable.empty
  override def localPath: String = ""

  val conf = new Configuration()

  def pathIsGood(p: String) = super.pathIsGood(testfsPathRoot + p, conf)
}
