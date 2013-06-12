package com.twitter.scalding.filecache

import java.io.File
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.specs.mock.Mockito
import org.specs.Specification
import com.twitter.scalding.{Test, Hdfs}

class DistributedCacheFileSpec extends Specification with Mockito {
  implicit val distCache = smartMock[DistributedCache]
  implicit val mode = smartMock[Hdfs]
  val conf = smartMock[Configuration]

  mode.conf returns conf
  mode.strict returns true

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

    "register the uri with the cache and return the appropriate CachedFile" in {
      val expectedUri = new URI("%s#%s".format(uriString, hashedFilename))

      val cf = dcf.add()

      there was one(distCache).createSymlink(conf)
      there was one(distCache).addCacheFile(expectedUri, conf)

      val cachedPath = "./" + hashedFilename
      cf.path must_== cachedPath
      cf.file must_== new File(cachedPath)
    }

    "throw HdfsNotAvailableException when the current mode isn't Hdfs" in {
      val mode = smartMock[Test]
      dcf.add()(mode) must throwA[HdfsNotAvailableException]
    }
  }

  "UncacheFile.addOpt" should {
    val dcf = new UncachedFile(Right(uri))

    "return Some(CachedFile) when the Mode is Hdfs" in {
      dcf.addOpt() must beSome[CachedFile]
    }

    "return None when Mode is not Hdfs" in {
      val mode = smartMock[Test]
      dcf.addOpt()(mode) must beNone
    }
  }
}
