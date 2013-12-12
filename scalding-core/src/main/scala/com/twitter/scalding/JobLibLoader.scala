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

package com.twitter.scalding

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}

object JobLibLoader  {

  def loadJars(libPathStr: String, config: Configuration) {
    try {
      val libPath: Path = new Path(libPathStr)
      val fs: FileSystem = FileSystem.get(config)
      val itr: RemoteIterator[LocatedFileStatus] = fs.listFiles(libPath, true)
      while (itr.hasNext) {
        val f: LocatedFileStatus = itr.next
        if (!f.isDirectory && f.getPath.getName.endsWith("jar")) {
          println("Loading Jar : " + f.getPath.getName)
          DistributedCache.addFileToClassPath(f.getPath, config,fs)
        } else {
          println("Loading static file : " + f.getPath.getName)
          DistributedCache.addCacheFile(f.getPath.toUri, config)
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
