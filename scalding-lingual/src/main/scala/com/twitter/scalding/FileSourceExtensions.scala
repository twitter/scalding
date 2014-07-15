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

import cascading.tap.Tap
import org.apache.hadoop.fs.{ Path, FileSystem }
import java.io.File

class FileSourceExtensions(val fs: FileSource) {
  def localTap = fs.createLocalTap(fs.sinkMode)

  def sourceTap(implicit mode: Mode): Tap[_, _, _] = fs.createTap(Read)
  def sinkTap(implicit mode: Mode): Tap[_, _, _] = fs.createTap(Write)

  def delete(implicit mode: Mode) {
    mode match {
      case Hdfs(_, jc) => fs.hdfsPaths.map(f => FileSystem.get(jc).delete(new Path(f), true))
      case Local(_) => new File(fs.localPath).delete()
      case _ => sys.error("Unable to delete the file")
    }
  }
}

object FileSourceExtensions {
  implicit def fileSourceToExtendedFileSource(fs: FileSource) = new FileSourceExtensions(fs)
}