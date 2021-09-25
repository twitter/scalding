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
package com.twitter.scalding.filecache

import cascading.tuple.Tuple
import com.twitter.scalding._
import java.net.URI
import org.scalatest.{ Matchers, WordSpec }
import scala.collection.mutable

// TODO: fix? is it worth having the dep on mockito just for this?
class DistributedCacheFileSpec extends WordSpec with Matchers {
  case class UnknownMode(buffers: Map[Source, mutable.Buffer[Tuple]]) extends TestMode with CascadingLocal
  /*
  val conf = smartMock[Configuration]

  lazy val hdfsMode = {
    val mode = smartMock[Hdfs]
    mode.conf returns conf
    mode.strict returns true
    mode
  }

  lazy val hadoopTestMode = {
    val mode = smartMock[HadoopTest]
    mode.conf returns conf
    mode
  }

  lazy val testMode = smartMock[Test]
  lazy val localMode = smartMock[Local]
*/
  val uriString = "hdfs://foo.example:1234/path/to/the/stuff/thefilename.blah"
  val uri = new URI(uriString)
  val hashHex = URIHasher(uri)
  val hashedFilename = hashHex + "-thefilename.blah"

  "DistributedCacheFile" should {
    "symlinkNameFor must return a hashed name" in {
      DistributedCacheFile.symlinkNameFor(uri) shouldBe hashedFilename
    }
  }

  /*
  "UncachedFile.add" should {
    val dcf = new UncachedFile(Right(uri))

    def sharedLocalBehavior(implicit mode: Mode) = {
      "use the local file path" in {
        val cf = dcf.add()(mode)

        cf.path shouldBe (uri.getPath)
        cf.file shouldBe (new File(uri.getPath).getCanonicalFile)
      }
    }

    "with a Test mode" in {
      sharedLocalBehavior(testMode)
    }

    "with a Local mode" in {
      sharedLocalBehavior(localMode)
    }

    "throw RuntimeException when the current mode isn't recognized" in {
      val mode = smartMock[UnknownMode]
      an[RuntimeException] should be thrownBy (dcf.add()(mode))
    }
  }
  */
}
