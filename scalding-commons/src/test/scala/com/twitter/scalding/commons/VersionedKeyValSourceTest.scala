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

import org.specs._
import com.twitter.scalding._
import com.twitter.bijection.Injection
import com.google.common.io.Files
import com.backtype.hadoop.datastores.VersionedStore
import org.apache.hadoop.mapred.JobConf

// Use the scalacheck generators
import org.scalacheck.Gen
import scala.collection.mutable.Buffer

import TDsl._

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

class VersionedKeyValSourceTest extends Specification {
  noDetailedDiffs()

  val input = (1 to 100).toList

  "A TypedWriteIncrementalJob" should {
    JobTest(new TypedWriteIncrementalJob(_))
      .source(TypedTsv[Int]("input"), input)
      .sink[(Int, Int)](VersionedKeyValSource[Array[Byte], Array[Byte]]("output")) { outputBuffer: Buffer[(Int, Int)] =>
        "Outputs must be as expected" in {
          outputBuffer.size must_== input.size
          val singleInj = implicitly[Injection[Int, Array[Byte]]]
          input.map{ k => (k, k) }.sortBy(_._1).toString must be_==(outputBuffer.sortBy(_._1).toList.toString)
        }
      }
      .run
      .finish
  }

  "A MoreComplexTypedWriteIncrementalJob" should {
    JobTest(new MoreComplexTypedWriteIncrementalJob(_))
      .source(TypedTsv[Int]("input"), input)
      .sink[(Int, Int)](VersionedKeyValSource[Array[Byte], Array[Byte]]("output")) { outputBuffer: Buffer[(Int, Int)] =>
        "Outputs must be as expected" in {
          outputBuffer.size must_== input.size
          val singleInj = implicitly[Injection[Int, Array[Byte]]]
          input.map{ k => (k, k) }.sortBy(_._1).toString must be_==(outputBuffer.sortBy(_._1).toList.toString)
        }
      }
      .run
      .finish
  }

  "A VersionedKeyValSource" should {
    "Validate that explicitly provided versions exist" in {
      val path = setupLocalVersionStore(100L to 102L)

      validateVersion(path, Some(103)) must throwA(
        new InvalidSourceException("Version 103 does not exist. " +
          "Currently available versions are: [102, 101, 100]"))

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
      store.succeedVersion(p)
    }

    root.getAbsolutePath
  }

  /**
   * Creates a VersionedKeyValSource using the provided version
   * and then validates it.
   */
  private def validateVersion(path: String, version: Option[Long] = None) = {
    VersionedKeyValSource(path = path, sourceVersion = version)
      .validateTaps(Hdfs(false, new JobConf()))
  }
}
