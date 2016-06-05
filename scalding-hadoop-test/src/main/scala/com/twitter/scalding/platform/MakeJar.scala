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
package com.twitter.scalding.platform

import java.io.{ BufferedInputStream, File, FileInputStream, FileOutputStream }
import java.util.jar.{ Attributes, JarEntry, JarOutputStream, Manifest => JarManifest }

import org.slf4j.LoggerFactory

object MakeJar {
  private val LOG = LoggerFactory.getLogger(getClass)

  def apply(classDir: File, jarName: Option[String] = None): File = {
    val syntheticJar = new File(
      System.getProperty("java.io.tmpdir"),
      jarName.getOrElse(classDir.getAbsolutePath.replace("/", "_") + ".jar"))
    LOG.debug("Creating synthetic jar: " + syntheticJar.getAbsolutePath)
    val manifest = new JarManifest
    manifest.getMainAttributes.put(Attributes.Name.MANIFEST_VERSION, "1.0")
    val target = new JarOutputStream(new FileOutputStream(syntheticJar), manifest)
    add(classDir, classDir, target)
    target.close()
    new File(syntheticJar.getAbsolutePath)
  }

  private[this] def add(parent: File, source: File, target: JarOutputStream): Unit = {
    val name = getRelativeFileBetween(parent, source).getOrElse(new File("")).getPath.replace("\\", "/")
    if (source.isDirectory) {
      if (!name.isEmpty) {
        val entry = new JarEntry(if (!name.endsWith("/")) name + "/" else name)
        entry.setTime(source.lastModified())
        target.putNextEntry(entry)
        target.closeEntry()
      }
      source.listFiles.foreach { add(parent, _, target) }
    } else {
      val entry = new JarEntry(name)
      entry.setTime(source.lastModified)
      target.putNextEntry(entry)
      val in = new BufferedInputStream(new FileInputStream(source))
      val buffer = new Array[Byte](1024)
      var count = in.read(buffer)
      while (count > -1) {
        target.write(buffer, 0, count)
        count = in.read(buffer)
      }
      target.closeEntry
      in.close()
    }
  }

  // Note that this assumes that parent and source are in absolute form if that's what we want
  @annotation.tailrec
  private[this] def getRelativeFileBetween(
    parent: File, source: File, result: List[String] = List.empty): Option[File] =
    Option(source) match {
      case Some(src) => {
        if (parent == src) {
          result.foldLeft(None: Option[File]) { (cum, part) =>
            Some(cum match {
              case Some(p) => new File(p, part)
              case None => new File(part)
            })
          }
        } else {
          getRelativeFileBetween(parent, src.getParentFile, src.getName :: result)
        }
      }
      case None => None
    }
}
