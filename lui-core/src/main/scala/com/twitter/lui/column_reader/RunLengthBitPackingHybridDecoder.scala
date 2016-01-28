/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.twitter.lui.column_reader

import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.io.IOException

import org.apache.parquet.Log
import org.apache.parquet.Preconditions
import org.apache.parquet.bytes.BytesUtils
import org.apache.parquet.column.values.bitpacking.BytePacker
import org.apache.parquet.column.values.bitpacking.Packer
import org.apache.parquet.io.ParquetDecodingException

/**
 * Decodes values written in the grammar described in {@link RunLengthBitPackingHybridEncoder}
 *
 */
object RunLengthBitPackingHybridDecoder {
  private val LOG: Log = Log.getLog(classOf[RunLengthBitPackingHybridDecoder])
  private[RunLengthBitPackingHybridDecoder] sealed trait MODE
  private[RunLengthBitPackingHybridDecoder] case object RLE extends MODE
  private[RunLengthBitPackingHybridDecoder] case object PACKED extends MODE
  private[RunLengthBitPackingHybridDecoder] case object COMPLETE extends MODE
}
class RunLengthBitPackingHybridDecoder(bitWidth: Int, backing: Array[Byte], var offset: Int, limit: Int) {
  import RunLengthBitPackingHybridDecoder._
  Preconditions.checkArgument(bitWidth >= 0 && bitWidth <= 32, "bitWidth must be >= 0 and <= 32")

  private[this] val bytesWidth: Int = BytesUtils.paddedByteCountFromBits(bitWidth)
  private[this] val packer: BytePacker = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth)

  private[this] var mode: MODE = RLE
  private[this] var currentBuffer = new Array[Int](10)
  private[this] var bufferLength = 10
  private[this] var readIdx = 0
  private[this] var currentCount = 0

  def readInt(): Int = {
    if (readIdx == currentCount) {
      readNext()
    }
    mode match {
      case RLE =>
        readIdx += 1
        currentBuffer(0)
      case PACKED =>
        readIdx += 1
        currentBuffer(readIdx - 1)
      case _ =>
        throw new ParquetDecodingException("EOF finished file")
    }
  }

  def readInts(target: Array[Int]): Unit = {
    var pos = 0
    var rIdx = readIdx
    var curCnt = currentCount
    while (pos < target.length) {
      if (rIdx == curCnt) {
        readNext()
        rIdx = 0
        curCnt = currentCount
        if (curCnt == 0) // EOF
          return
      }
      if (mode == RLE) {
        while (pos < target.length && rIdx < curCnt) {
          target(pos) = currentBuffer(0)
          pos += 1
          rIdx += 1
        }
      } else {
        while (pos < target.length && rIdx < curCnt) {
          target(pos) = currentBuffer(rIdx)
          pos += 1
          rIdx += 1
        }
      }
    }
    readIdx = rIdx
  }

  private[this] def readUnsignedVarInt(): Int = {
    var value = 0
    var i = 0
    var b = 0
    def updateB(): Int = {
      b = backing(offset)
      offset += 1
      b
    }

    while ((updateB() & 0x80) != 0) {
      value |= (b & 0x7F) << i
      i += 7
    }
    value | (b << i)
  }

  private[this] def readIntLittleEndianPaddedOnBitWidth(): Int = {
    val bw: Int = bytesWidth
    var res = 0
    var o = offset
    if (bw > 0) {
      res += backing(o) & 0xff
      o += 1
      if (bw > 1) {
        res += (backing(o) & 0xff) << 8
        o += 1
        if (bw > 2) {
          res += (backing(o) & 0xff) << 16
          o += 1
          if (bw > 3) {
            res += (backing(o) & 0xff) << 24
            o += 1
          }
        }
      }
    }
    offset = o
    res
  }

  private[this] def growBuffer(requestedSize: Int): Unit = {
    val oldLength = bufferLength
    bufferLength = (requestedSize * 1.5).toInt
    val oldBuffer = currentBuffer
    currentBuffer = new Array[Int](bufferLength)
    System.arraycopy(oldBuffer, 0, currentBuffer, 0, oldLength)
  }

  private[this] final def readNext(): Unit = {
    if (offset >= limit) {
      currentCount = 0
      mode = COMPLETE
      return
    }

    val header = readUnsignedVarInt()
    mode = if ((header & 1) == 0) RLE else PACKED
    readIdx = 0
    mode match {
      case RLE =>
        currentCount = header >>> 1
        currentBuffer(0) = readIntLittleEndianPaddedOnBitWidth()
      case PACKED =>
        val numGroups = header >>> 1
        val nextCount = numGroups * 8
        var valueIndex = 0
        currentCount = nextCount

        if (bufferLength < currentCount) {
          growBuffer(currentCount)
        }

        // At the end of the file RLE data though, there might not be that many bytes left.
        val tmp: Int = nextCount * bitWidth
        val tmp2: Int = tmp / 8
        var bytesToRead: Int = if (tmp % 8 == 0) tmp2 else tmp2 + 1
        val remaining = limit - offset
        bytesToRead = if (remaining < bytesToRead) remaining else bytesToRead

        while (valueIndex < currentCount) {
          packer.unpack8Values(backing, offset, currentBuffer, valueIndex)
          valueIndex += 8
          offset += bitWidth
        }
      case _ =>
        throw new ParquetDecodingException("not a valid mode " + mode)
    }
  }
}
