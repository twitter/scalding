package com.twitter.scalding.filecache

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.fs._

object DistributedCacheClasspath {

  def loadJars(libPathStr: String, config: Configuration) {
    try {
      val libPath = new Path(libPathStr)
      val fs = FileSystem.get(config)
      val status = fs.listStatus(libPath)
      for (i<- 0 to status.length) {
        //val f: LocatedFileStatus = itr.next
        if (!status(i).isDir && status(i).getPath.getName.endsWith("jar")) {
          println("Loading Jar : " + status(i).getPath.getName)
          DistributedCache.addFileToClassPath(status(i).getPath, config,fs)
        } else {
          println("Loading static file : " + status(i).getPath.getName)
          DistributedCache.addCacheFile(status(i).getPath.toUri, config)
        }
      }
    }
    catch {
      case e: Exception => {
        println("JobLibLoader error in loadJars")
        e.printStackTrace
      }
    }
  }

  def addFiletoCache(libPathStr: String, config: Configuration) {
    try {
      val filePath: Path = new Path(libPathStr)
      DistributedCache.addCacheFile(filePath.toUri, config)
    }
    catch {
      case e: Exception => {
        println("JobLibLoader error in addFiletoCache")
        e.printStackTrace

      }
    }
  }

  def getFileFromCache(libPathStr: String, config: Configuration): Array[Path] = {
    var localFiles: Array[Path] = null
    try {
      println("Local Cache => " + DistributedCache.getLocalCacheFiles(config))
      println("Hadoop Cache => " + DistributedCache.getCacheFiles(config))
      if (DistributedCache.getLocalCacheFiles(config) != null) {
        localFiles = DistributedCache.getLocalCacheFiles(config)
      }
      println("LocalFiles => " + localFiles)
    }
    catch {
      case e: Exception => {
        println("JobLibLoader error in getFileFromCache")
        e.printStackTrace

      }
    }
    return localFiles
  }

}
