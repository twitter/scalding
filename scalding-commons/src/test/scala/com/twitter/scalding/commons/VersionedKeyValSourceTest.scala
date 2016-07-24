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

import org.scalatest.{ Matchers, WordSpec }
import com.twitter.scalding._
import com.twitter.scalding.commons.datastores.VersionedStore;
import com.twitter.scalding.typed.IterablePipe
import com.twitter.bijection.Injection
import com.google.common.io.Files
import org.apache.hadoop.mapred.JobConf

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
          val singleInj = implicitly[Injection[Int, Array[Byte]]]
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
          val singleInj = implicitly[Injection[Int, Array[Byte]]]
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
      store.succeedVersion(p)
    }

    root.getAbsolutePath
  }

  /**
   * Creates a VersionedKeyValSource using the provided version
   * and then validates it.
   */
  private def validateVersion(path: String, version: Option[Long] = None) =
    VersionedKeyValSource(path = path, sourceVersion = version)
      .validateTaps(Hdfs(false, new JobConf()))
}
