package com.twitter.scalding.filecache

import com.google.common.hash.Hashing
import java.io.File
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.specs.mock.Mockito
import org.specs.Specification

class DistributedCacheFileSpec extends Specification with Mockito {
  implicit val distCache = smartMock[DistributedCache]
  val conf = smartMock[Configuration]
  val uriString = "hdfs://foo.example:1234/path/to/the/stuff/thefilename.blah"
  val md5Hex = Hashing.md5().hashString(uriString).toString
  val hashedFilename = "thefilename.blah-" + md5Hex
  val uri = new URI(uriString)


  distCache.makeQualified(uri, conf) returns uri
  distCache.makeQualified(uriString, conf) returns uri

  "DistributedCacheFile" should {
    "symlinkNameFor must return a hashed name" in {
      DistributedCacheFile.symlinkNameFor(uri) must_== hashedFilename
    }
  }

  "UncachedFile" should {
    "not be defined" in {
      DistributedCacheFile(uri).isDefined must beTrue
    }
  }

  "UncachedFile.add" should {
    "register the uri with the cache and return the appropriate CachedFile" in {
      val expectedUri = new URI("%s#%s".format(uriString, hashedFilename))

      val dcf = new UncachedFile(Right(uri))
      val cf = dcf.add(conf)

      there was one(distCache).createSymlink(conf)
      there was one(distCache).addCacheFile(expectedUri, conf)

      val cachedPath = "./" + hashedFilename
      cf.path must_== cachedPath
      cf.file must_== (new File(cachedPath))
    }
  }
}
