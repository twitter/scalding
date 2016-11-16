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
package com.twitter.scalding.commons.source

import org.apache.hadoop.fs.Path
import org.scalatest.{ Matchers, WordSpec }
import com.twitter.scalding._
import com.twitter.scalding.commons.datastores.VersionedStore
import com.twitter.scalding.typed.IterablePipe
import com.twitter.bijection.Injection
import com.google.common.io.Files
import org.apache.hadoop.mapred.{SequenceFileInputFormat, JobConf}

import java.io.File
// Use the scalacheck generators
import scala.collection.mutable.Buffer

class TypedWriteIncrementalJob(args: Args) extends Job(args) {
  import RichPipeEx._
  val pipe = TypedPipe.from(TypedTsv[Int]("input"))

  implicit val inj = Injection.connect[(Int, Int), (Array[Byte], Array[Byte])]

  pipe
    .map{ k => (k, k) }
    .writeIncremental(VersionedKeyValSource[Int, Int]("output"))
}

// Test version of SequenceFileInputFormat to get details on which
// paths it will use
class TestSequenceFileInputFormat extends SequenceFileInputFormat[Int, Int] {
  def getPaths(conf: JobConf): Array[Path] = super.listStatus(conf).map(_.getPath)
}

class MoreComplexTypedWriteIncrementalJob(args: Args) extends Job(args) {
  import RichPipeEx._
  val pipe = TypedPipe.from(TypedTsv[Int]("input"))

  implicit val inj = Injection.connect[(Int, Int), (Array[Byte], Array[Byte])]

  pipe
    .map{ k => (k, k) }
    .group
    .sum
    .writeIncremental(VersionedKeyValSource[Int, Int]("output"))
}

class ToIteratorJob(args: Args) extends Job(args) {
  import RichPipeEx._
  val source = VersionedKeyValSource[Int, Int]("input")

  val iteratorCopy = source.toIterator.toList
  val iteratorPipe = IterablePipe(iteratorCopy)

  val duplicatedPipe = TypedPipe.from(source) ++ iteratorPipe

  duplicatedPipe
    .group
    .sum
    .writeIncremental(VersionedKeyValSource[Int, Int]("output"))
}

class VersionedKeyValSourceTest extends WordSpec with Matchers {
  val input = (1 to 100).toList

  "A TypedWriteIncrementalJob" should {
    JobTest(new TypedWriteIncrementalJob(_))
      .source(TypedTsv[Int]("input"), input)
      .sink[(Int, Int)](VersionedKeyValSource[Array[Byte], Array[Byte]]("output")) { outputBuffer: Buffer[(Int, Int)] =>
        "Outputs must be as expected" in {
          assert(outputBuffer.size === input.size)
          assert(input.map{ k => (k, k) }.sortBy(_._1).toString === outputBuffer.sortBy(_._1).toList.toString)
        }
      }
      .run
      .finish()
  }

  "A MoreComplexTypedWriteIncrementalJob" should {
    JobTest(new MoreComplexTypedWriteIncrementalJob(_))
      .source(TypedTsv[Int]("input"), input)
      .sink[(Int, Int)](VersionedKeyValSource[Array[Byte], Array[Byte]]("output")) { outputBuffer: Buffer[(Int, Int)] =>
        "Outputs must be as expected" in {
          assert(outputBuffer.size === input.size)
          assert(input.map{ k => (k, k) }.sortBy(_._1).toString === outputBuffer.sortBy(_._1).toList.toString)
        }
      }
      .run
      .finish()
  }

  "A ToIteratorJob" should {
    "return the values via toIterator" in {
      JobTest(new ToIteratorJob(_))
        .source(VersionedKeyValSource[Int, Int]("input"), input.zip(input))
        .sink(VersionedKeyValSource[Int, Int]("output")) { outputBuffer: Buffer[(Int, Int)] =>
          val (keys, vals) = outputBuffer.unzip
          assert(keys.map { _ * 2 } === vals)
        }
        .run
        .finish()
    }
  }

  "A VersionedKeyValSource" should {
    "Validate that explicitly provided versions exist" in {
      val path = setupLocalVersionStore(100L to 102L)

      val thrown = the[InvalidSourceException] thrownBy { validateVersion(path, Some(103)) }
      assert(thrown.getMessage === "Version 103 does not exist. " +
        "Currently available versions are: [102, 101, 100]")

      // should not throw
      validateVersion(path, Some(101))

      // should not throw
      validateVersion(path)
    }
  }

  /**
   * Creates a temp dir and then creates the provided versions within it.
   */
  private def setupLocalVersionStore(versions: Seq[Long]): String = {
    val root = Files.createTempDir()
    root.deleteOnExit()
    val store = new VersionedStore(root.getAbsolutePath)
    versions foreach { v =>
      val p = store.createVersion(v)
      new File(p).mkdirs()
      // create a part file here
      new File(p + "/part-00000").createNewFile()
      // and succeed
      store.succeedVersion(p)
    }

    root.getAbsolutePath
  }

  /**
   * Creates a VersionedKeyValSource using the provided version
   * and then validates it.
   */
  private def validateVersion(path: String, version: Option[Long] = None) = {
    val store = VersionedKeyValSource(path = path, sourceVersion = version)
    val conf: JobConf = new JobConf()
    store.validateTaps(Hdfs(strict = false, conf))

    // also validate the paths for the version
    validateVersionPaths(path, version, store, conf)
  }

  def validateVersionPaths(path: String, version: Option[Long], store: VersionedKeyValSource[_, _], conf: JobConf): Unit = {
    store.source.sourceConfInit(null, conf) // this sets up the splits needed for input format
    val fileInputFormat = new TestSequenceFileInputFormat()
    val paths = fileInputFormat.getPaths(conf)
    version match {
      case Some(ver) =>
        // expect only the part file for the specified version
        assert(paths.length == paths.count(_.toString.endsWith(ver + "/part-00000")))
      case None =>
        // when no version is specified, we get the most recent version's data
        val mostRecentVersion = store.source.getStore(conf).mostRecentVersion()
        assert(paths.length == paths.count(_.toString.endsWith(mostRecentVersion + "/part-00000")))
    }
  }
}
