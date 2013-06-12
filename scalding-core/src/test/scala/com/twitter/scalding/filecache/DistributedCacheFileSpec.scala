package com.twitter.scalding.filecache

import cascading.tuple.Tuple
import com.twitter.scalding._
import java.io.File
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.specs.Specification
import org.specs.mock.Mockito
import scala.collection.mutable


class DistributedCacheFileSpec extends Specification with Mockito {
  case class UnknownMode(buffers: Map[Source, mutable.Buffer[Tuple]]) extends Mode(false)
      with TestMode
      with CascadingLocal

  implicit val distCache = smartMock[DistributedCache]
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

  val uriString = "hdfs://foo.example:1234/path/to/the/stuff/thefilename.blah"
  val uri = new URI(uriString)
  val hashHex = URIHasher(uri)
  val hashedFilename = "thefilename.blah-" + hashHex

  distCache.makeQualified(uri, conf) returns uri
  distCache.makeQualified(uriString, conf) returns uri

  "DistributedCacheFile" should {
    "symlinkNameFor must return a hashed name" in {
      DistributedCacheFile.symlinkNameFor(uri) must_== hashedFilename
    }
  }

  "UncachedFile" should {
    "not be defined" in {
      DistributedCacheFile(uri).isDefined must beFalse
    }
  }

  "UncachedFile.add" should {
    val dcf = new UncachedFile(Right(uri))

    def sharedHdfsBehavior(implicit mode: Mode) = {
      "register the uri with the cache and return the appropriate CachedFile" in {
        val expectedUri = new URI("%s#%s".format(uriString, hashedFilename))

        val cf = dcf.add()

        there was one(distCache).createSymlink(conf)
        there was one(distCache).addCacheFile(expectedUri, conf)

        val cachedPath = "./" + hashedFilename
        cf.path must_== cachedPath
        cf.file must_== new File(cachedPath)
      }
    }

    "with an Hdfs mode" in {
      sharedHdfsBehavior(hdfsMode)
    }

    "with a HadoopTest mode" in {
      sharedHdfsBehavior(hadoopTestMode)
    }

    def sharedLocalBehavior(implicit mode: Mode) = {
      "use the local file path" in {
        val cf = dcf.add()(mode)

        cf.path must_== uri.getPath
        cf.file must_== new File(uri.getPath).getCanonicalFile
      }
    }

    "with a Test mode" in {
      sharedLocalBehavior(testMode)
    }

    "with a  Local mode" in {
      sharedLocalBehavior(localMode)
    }


    "throw RuntimeException when the current mode isn't recognized" in {
      val mode = smartMock[UnknownMode]
      dcf.add()(mode) must throwA[RuntimeException]
    }
  }

  "UncachedFile.addOpt" should {
    val dcf = new UncachedFile(Right(uri))

    "return Some(CachedFile) when the Mode is recognized" in {
      dcf.addOpt()(hdfsMode) must beSome[CachedFile]
    }

    "return None when Mode is not known" in {
      val mode = smartMock[UnknownMode]
      dcf.addOpt()(mode) must beNone
    }
  }
}
