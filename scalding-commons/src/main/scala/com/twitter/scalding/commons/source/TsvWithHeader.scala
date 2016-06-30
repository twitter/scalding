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

package com.twitter.scalding.commons.source

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.tuple.Fields
import com.google.common.base.Charsets
import com.google.common.io.Files
import com.twitter.scalding._
import java.io.{ BufferedWriter, File, FileOutputStream, IOException, OutputStreamWriter }
import org.apache.hadoop.fs.{ FileSystem, Path }

/**
 * A tsv source with the column name header info.
 *
 * Header file format: tab separated column names.
 */
class TsvWithHeader(p: String, f: Fields = Fields.UNKNOWN)(implicit mode: Mode)
  extends FixedPathSource(p)
  with DelimitedScheme
  with FieldConversions {
  val headerPath = p.replaceAll("/+$", "") + ".HEADER"

  // make it lazy so as to only do once
  lazy val fieldsFromHeaderFile = {
    val names = readFromFile(headerPath)
      .split("\t")
      .toSeq
    new Fields(names: _*)
  }

  override val fields = if (f == Fields.UNKNOWN) {
    fieldsFromHeaderFile
  } else {
    f
  }

  // TODO: move this method to make it a util function.
  def readFromFile(filename: String)(implicit mode: Mode) = {
    mode match {
      case Hdfs(_, conf) => {
        try {
          val pt = new Path(filename)
          val fs = pt.getFileSystem(conf)
          fs.open(pt).readUTF
        } catch {
          case e: IOException => {
            throw new RuntimeException(e)
          }
        }
      }
      // Local mode
      case _ => {
        try {
          Files.toString(new File(filename), Charsets.UTF_8)
        } catch {
          case e: IOException => {
            throw new RuntimeException(e)
          }
        }
      }
    }
  }

  // TODO: move this method to make it a util function.
  def writeToFile(filename: String, text: String)(implicit mode: Mode): Unit = {
    mode match {
      case Hdfs(_, conf) => {
        try {
          val pt = new Path(filename)
          val fs = pt.getFileSystem(conf)
          val br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)))

          br.write(text)
          br.close()
        } catch {
          case e: IOException => {
            throw new RuntimeException(e)
          }
        }
      }
      // Local mode
      case _ => {
        try {
          val br = new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream(filename), "utf-8"))

          br.write(text)
          br.close()
        } catch {
          case e: IOException => {
            throw new RuntimeException(e)
          }
        }
      }
    }
  }

  override def writeFrom(pipe: Pipe)(implicit flowDef: FlowDef, mode: Mode) = {
    val ret = super.writeFrom(pipe)(flowDef, mode)
    val fieldNames = for (i <- (0 until fields.size)) yield fields.get(i).asInstanceOf[String]
    val headerFileText = fieldNames.mkString("\t")
    writeToFile(headerPath, headerFileText)
    ret
  }
}
