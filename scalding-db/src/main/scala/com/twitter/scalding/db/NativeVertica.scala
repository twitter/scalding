/*
Copyright 2015 Twitter, Inc.

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

package com.twitter.scalding.db

import com.twitter.scalding.db.extensions.VerticaExtensions
import scala.util.{ Try, Success, Failure }
import java.io._

// To implement the headers from
// https://my.vertica.com/docs/CE/5.1.0/HTML/index.htm#13562.htm
object NativeVertica {

  import LittleEndianJavaStreamEnrichments._

  def headerFrom(columns: Iterable[ColumnDefinition]): Array[Byte] = {
    val preppedColumns = DBColumnTransformer.mutateColumns(VerticaExtensions.verticaMutator, columns)

    // put a 32bit int for size at the start...

    val baos = new ByteArrayOutputStream

    baos.writeShort(1) // formatVersion, must be written as a short. only 1 is defined
    baos.writeByte(0) // filler, always 1 byte
    baos.writeShort(preppedColumns.size.toShort) // number of columns as a short

    preppedColumns.foreach { col =>
      val colType = col.jdbcType
      colType match {
        case BIGINT => baos.writeInt(8) // 8 bytes in bigint
        case INT => baos.writeInt(4) // 4 bytes in an int
        case SMALLINT => baos.writeInt(2)
        case TINYINT => baos.writeInt(1)
        case BOOLEAN => baos.writeInt(1)
        case VARCHAR => baos.writeInt(-1)
        case DATE => baos.writeInt(8)
        case DATETIME => baos.writeInt(8)
        case TEXT => baos.writeInt(-1)
        case DOUBLE => baos.writeInt(8)
      }
    }

    val contentArray = baos.toByteArray

    val finalBaos = new ByteArrayOutputStream
    val signature = List(0x4E, 0x41, 0x54, 0x49, 0x56, 0x45, 0x0A, 0xFF, 0x0D, 0x0A, 0x00).map(_.toByte).toArray
    finalBaos.writeBytes(signature) // file signature
    finalBaos.writeInt(contentArray.size)
    finalBaos.writeBytes(contentArray)
    finalBaos.toByteArray
  }
}
