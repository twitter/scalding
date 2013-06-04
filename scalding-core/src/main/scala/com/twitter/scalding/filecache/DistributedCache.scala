package com.twitter.scalding.filecache

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.filecache.{DistributedCache => HDistributedCache}
import org.apache.hadoop.fs.Path

trait DistributedCache {
  def createSymlink(conf: Configuration)
  def addCacheFile(uri: URI, conf: Configuration)
  def makeQualified(path: String, conf: Configuration): URI
  def makeQualified(uri: URI, conf: Configuration): URI
  def makeQualified(p: Path, conf: Configuration): URI
}

// used to supply the implicit cache argument to UncachedFile, allows us to stub this in tests
class HadoopDistributedCache extends DistributedCache {
  def createSymlink(conf: Configuration) {
    HDistributedCache.createSymlink(conf)
  }

  def addCacheFile(uri: URI, conf: Configuration) {
    HDistributedCache.addCacheFile(uri, conf)
  }

  def makeQualified(path: String, conf: Configuration): URI =
    makeQualified(new Path(path), conf)

  def makeQualified(uri: URI, conf: Configuration) =
    makeQualified(new Path(uri.toString), conf) // uri.toString because hadoop 0.20.2 doesn't take a URI

  def makeQualified(p: Path, conf: Configuration): URI =
    p.makeQualified(p.getFileSystem(conf)).toUri  // make sure we have fully-qualified URI
}

