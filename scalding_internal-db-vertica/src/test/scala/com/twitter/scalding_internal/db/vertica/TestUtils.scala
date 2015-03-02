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
package com.twitter.scalding_internal.db.vertica

object TestUtils {
  def fetchLocalData(folderPath: String, fileName: String): List[Byte] = {
    val is = java.nio.file.Files.newInputStream(java.nio.file.FileSystems.getDefault().getPath(folderPath, fileName))
    val baos = new java.io.ByteArrayOutputStream()
    @annotation.tailrec
    def go(): Unit = {
      val b = is.read
      if (b >= 0) {
        baos.write(b.toByte)
        go
      }
    }
    go
    baos.toByteArray.toList
  }
}
