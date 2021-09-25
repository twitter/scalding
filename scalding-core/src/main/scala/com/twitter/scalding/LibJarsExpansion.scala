/*
Copyright 2014 Twitter, Inc.

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

import java.io.File
import java.nio.file.Path

object ExpandLibJarsGlobs {
  def apply(inputArgs: Array[String]): Array[String] = {
    // First we are going to expand out the libjars if we find it
    val libJarsIdx = inputArgs.indexOf("-libjars") + 1
    if (libJarsIdx > 0 && libJarsIdx < inputArgs.length) { // 0 would mean we never found -libjars
      val newArgs = new Array[String](inputArgs.length)
      System.arraycopy(inputArgs, 0, newArgs, 0, inputArgs.length)

      val existing = newArgs(libJarsIdx)
      val replacement = existing.split(",").flatMap { element =>
        fromGlob(element).map(_.toString)
      }.mkString(",")

      newArgs(libJarsIdx) = replacement
      newArgs
    } else inputArgs
  }

  //tree from Duncan McGregor @ http://stackoverflow.com/questions/2637643/how-do-i-list-all-files-in-a-subdirectory-in-scala
  private[this] def tree(root: File, skipHidden: Boolean = false): Stream[File] =
    if (!root.exists || (skipHidden && root.isHidden)) Stream.empty
    else root #:: (
      root.listFiles match {
        case null => Stream.empty
        case files => files.toStream.flatMap(tree(_, skipHidden))
      })

  def fromGlob(glob: String, filesOnly: Boolean = true): Stream[Path] = {
    import java.nio.file._
    val fs = FileSystems.getDefault()
    val expandedSlash = if (glob.endsWith("/")) s"${glob}/*" else glob
    val absoluteGlob = fs.getPath(expandedSlash).toAbsolutePath
    val matcher: PathMatcher = fs.getPathMatcher(s"glob:$absoluteGlob")

    val parentPath =
      if (absoluteGlob.getFileName.toString.contains("*")) absoluteGlob.getParent else absoluteGlob

    val pathStream = tree(parentPath.toFile).map(_.toPath)

    val globMatchingPaths = pathStream.filter(matcher.matches)

    if (filesOnly) globMatchingPaths.filter(_.toFile.isFile) else globMatchingPaths
  }
}