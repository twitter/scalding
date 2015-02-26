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

package com.twitter.scalding.serialization

import java.io.{ InputStream, OutputStream }
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

import JavaStreamEnrichments._

object StringOrderedSerialization {
  final def binaryIntCompare(leftSize: Int, seekingLeft: InputStream, rightSize: Int, seekingRight: InputStream): Int = {
    val toCheck = math.min(leftSize, rightSize)
    // we can check longs at a time this way:
    val longs = toCheck / 8

    @annotation.tailrec
    def compareLong(count: Int): Int =
      if (count == 0) 0
      else {
        val cmp = UnsignedComparisons.unsignedLongCompare(seekingLeft.readLong, seekingRight.readLong)
        if (cmp == 0) compareLong(count - 1)
        else cmp
      }

    /*
       * This algorithm only works if count in {0, 1, 2, 3}. Since we only
       * call it that way below it is safe.
       */
    def compareBytes(count: Int): Int =
      if ((count & 2) == 2) {
        // there are 2 or 3 bytes to read
        val cmp = Integer.compare(seekingLeft.readUnsignedShort,
          seekingRight.readUnsignedShort)
        if (cmp != 0) cmp
        else if (count == 3) Integer.compare(seekingLeft.readUnsignedByte,
          seekingRight.readUnsignedByte)
        else 0
      } else {
        // there are 0 or 1 bytes to read
        if (count == 0) 0
        else Integer.compare(seekingLeft.readUnsignedByte,
          seekingRight.readUnsignedByte)
      }

    /**
     * Now we start by comparing blocks of longs, then maybe 1 int, then 0 - 3 bytes
     */
    val lc = compareLong(longs)
    if (lc != 0) lc
    else {
      val remaining = (toCheck - 8 * longs)
      val read1Int = (remaining >= 4)

      val ic = if (read1Int) UnsignedComparisons.unsignedIntCompare(seekingLeft.readInt, seekingRight.readInt) else 0
      if (ic != 0) ic
      else {
        val bytes = remaining - (if (read1Int) 4 else 0)
        val bc = compareBytes(bytes)
        if (bc != 0) bc
        else {
          // the size is the fallback when the prefixes match:
          Integer.compare(leftSize, rightSize)
        }
      }
    }
  }
}

class StringOrderedSerialization extends OrderedSerialization[String] {
  import StringOrderedSerialization._
  override def hash(s: String) = s.hashCode
  override def compare(a: String, b: String) = a.compareTo(b)
  override def read(in: InputStream) = try {
    val byteString = new Array[Byte](in.readSize)
    in.readFully(byteString)
    Success(new String(byteString, "UTF-8"))
  } catch { case NonFatal(e) => Failure(e) }

  override def write(b: OutputStream, s: String) = try {
    val bytes = s.getBytes("UTF-8")
    b.writeSize(bytes.length)
    b.writeBytes(bytes)
    Serialization.successUnit
  } catch { case NonFatal(e) => Failure(e) }

  override def compareBinary(lhs: InputStream, rhs: InputStream) = try {
    val leftSize = lhs.readSize
    val rightSize = rhs.readSize

    val seekingLeft = PositionInputStream(lhs)
    val seekingRight = PositionInputStream(rhs)

    val leftStart = seekingLeft.position
    val rightStart = seekingRight.position

    val res = OrderedSerialization.resultFrom(binaryIntCompare(leftSize, seekingLeft, rightSize, seekingRight))
    seekingLeft.seekToPosition(leftStart + leftSize)
    seekingRight.seekToPosition(rightStart + rightSize)
    res
  }
}
