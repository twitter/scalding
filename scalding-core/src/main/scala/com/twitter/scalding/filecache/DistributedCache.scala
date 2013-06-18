package com.twitter.scalding.filecache

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.filecache.{DistributedCache => HDistributedCache}
import org.apache.hadoop.fs.Path


/**
 * To use the DistributedCache, do DistributedCacheFile("/path/to/your/file.txt").
 *
 * {{{
 * class YourJob(args: Args) extends Job(args) with HadoopDistributedCache {
 *   val theCachedFile = DistributedCacheFile("hdfs://ur-namenode/path/to/your/file.txt")
 *
 *   def somethingThatUsesTheCachedFile() {
 *     doSomethingWith(theCachedFile.path) // or theCachedFile.file
 *   }
 * }
 * }}}
 */
trait DistributedCache {
  def createSymlink(conf: Configuration) {
    HDistributedCache.createSymlink(conf)
  }

  def addCacheFile(uri: URI, conf: Configuration) {
    HDistributedCache.addCacheFile(uri, conf)
  }

  def makeQualified(path: String, conf: Configuration): URI =
    makeQualified(new Path(path), conf)

  def makeQualified(uri: URI, conf: Configuration): URI =
    makeQualified(new Path(uri.toString), conf) // uri.toString because hadoop 0.20.2 doesn't take a URI

  def makeQualified(p: Path, conf: Configuration): URI =
    p.makeQualified(p.getFileSystem(conf)).toUri  // make sure we have fully-qualified URI
}

class HadoopDistributedCache extends DistributedCache
