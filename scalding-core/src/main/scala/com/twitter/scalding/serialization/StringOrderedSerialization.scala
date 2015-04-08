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
import scala.util.{ Failure, Success }
import scala.util.control.NonFatal

import JavaStreamEnrichments._

object StringOrderedSerialization {
  final def binaryIntCompare(leftSize: Int, seekingLeft: InputStream, rightSize: Int, seekingRight: InputStream): Int = {
    /*
       * This algorithm only works if count in {0, 1, 2, 3}. Since we only
       * call it that way below it is safe.
       */

    @inline
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
     * Now we start by comparing blocks of ints, then 0 - 3 bytes
     */
    val toCheck = math.min(leftSize, rightSize)
    val ints = toCheck / 4
    var counter = ints
    var ic = 0
    while ((counter > 0) && (ic == 0)) {
      // Unsigned compare of ints is cheaper than longs, because we can do it
      // by upcasting to Long
      ic = UnsignedComparisons.unsignedIntCompare(seekingLeft.readInt, seekingRight.readInt)
      counter = counter - 1
    }
    if (ic != 0) ic
    else {
      val bc = compareBytes(toCheck - 4 * ints)
      if (bc != 0) bc
      else {
        // the size is the fallback when the prefixes match:
        Integer.compare(leftSize, rightSize)
      }
    }
  }
}

class StringOrderedSerialization extends OrderedSerialization[String] {
  import StringOrderedSerialization._
  override def hash(s: String) = s.hashCode
  override def compare(a: String, b: String) = a.compareTo(b)
  override def read(in: InputStream) = try {
    val byteString = new Array[Byte](in.readPosVarInt)
    in.readFully(byteString)
    Success(new String(byteString, "UTF-8"))
  } catch { case NonFatal(e) => Failure(e) }

  override def write(b: OutputStream, s: String) = try {
    val bytes = s.getBytes("UTF-8")
    b.writePosVarInt(bytes.length)
    b.writeBytes(bytes)
    Serialization.successUnit
  } catch { case NonFatal(e) => Failure(e) }

  override def compareBinary(lhs: InputStream, rhs: InputStream) = try {
    val leftSize = lhs.readPosVarInt
    val rightSize = rhs.readPosVarInt

    val seekingLeft = PositionInputStream(lhs)
    val seekingRight = PositionInputStream(rhs)

    val leftStart = seekingLeft.position
    val rightStart = seekingRight.position

    val res = OrderedSerialization.resultFrom(binaryIntCompare(leftSize, seekingLeft, rightSize, seekingRight))
    seekingLeft.seekToPosition(leftStart + leftSize)
    seekingRight.seekToPosition(rightStart + rightSize)
    res
  } catch {
    case NonFatal(e) => OrderedSerialization.CompareFailure(e)
  }
  /**
   * generally there is no way to see how big a utf-8 string is without serializing.
   * We could scan looking for all ascii characters, but it's hard to see if
   * we'd get the balance right.
   */
  override def staticSize = None
  override def dynamicSize(s: String) = None
}
