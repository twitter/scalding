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

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream, EOFException }

object LittleEndianJavaStreamEnrichments {
  val JanFirst2kMillisSinceEpoch = 946684800000L

  def eof: Nothing = throw new EOFException()

  implicit class RichByteArrayOutputStream(val baos: ByteArrayOutputStream) extends AnyVal {
    def toInputStream: ByteArrayInputStream = new ByteArrayInputStream(baos.toByteArray)
  }

  /**
   * This has a lot of methods from DataInputStream without
   * having to allocate to get them
   * This code is similar to those algorithms
   */
  implicit class RichInputStream(val s: InputStream) extends AnyVal {
    /**
     * If s supports marking, we mark it. Otherwise we read the needed
     * bytes out into a ByteArrayStream and return that.
     * This is intended for the case where you need possibly
     * read size bytes but may stop early, then skip this exact
     * number of bytes.
     * Intended use is:
     * {code}
     * val size = 100
     * val marked = s.markOrBuffer(size)
     * val y = fn(marked)
     * marked.reset
     * marked.skipFully(size)
     * {/code}
     */
    def markOrBuffer(size: Int): InputStream = {
      val ms = if (s.markSupported) s else {
        val buf = new Array[Byte](size)
        s.readFully(buf)
        new ByteArrayInputStream(buf)
      }
      // Make sure we can reset after we read this many bytes
      ms.mark(size)
      ms
    }

    def readBoolean: Boolean = (readUnsignedByte != 0)

    def readBytes(numBytes: Int): Array[Byte] = {
      val arr = new Array[Byte](numBytes)
      if (numBytes > 0) {
        val read = s.read(arr)
        if (read != numBytes) sys.error(s"Read back fewer bytes $read than expected $numBytes")
      }
      arr
    }
    /**
     * Like read, but throws eof on error
     */
    def readByte: Byte = readUnsignedByte.toByte

    def readUnsignedByte: Int = {
      // Note that Java, when you read a byte, returns a Int holding an unsigned byte.
      // if the value is < 0, you hit EOF.
      val c1 = s.read
      if (c1 < 0) eof else c1
    }
    def readUnsignedShort: Int = {
      val c1 = s.read
      val c2 = s.read
      if ((c1 | c2) < 0) eof else ((c2 << 8) | c1)
    }

    final def readFully(bytes: Array[Byte]): Unit = readFully(bytes, 0, bytes.length)

    final def readFully(bytes: Array[Byte], offset: Int, len: Int): Unit = {
      if (len < 0) throw new IndexOutOfBoundsException()

      @annotation.tailrec
      def go(o: Int, l: Int): Unit =
        if (l == 0) ()
        else {
          val count = s.read(bytes, o, l)
          if (count < 0) eof
          else go(o + count, l - count)
        }
      go(offset, len)
    }

    def readDouble: Double = java.lang.Double.longBitsToDouble(readLong)
    def readFloat: Float = java.lang.Float.intBitsToFloat(readInt)

    def readInt: Int = {
      val c4 = s.read
      val c3 = s.read
      val c2 = s.read
      val c1 = s.read

      // This is the algorithm from DataInputStream
      if ((c1 | c2 | c3 | c4) < 0) eof else ((c1 << 24) | (c2 << 16) | (c3 << 8) | c4)
    }

    def readLong: Long = {
      // This is the algorithm from DataInputStream
      val buf = new Array[Byte](8)
      readFully(buf)
      (buf(7).toLong << 56) +
        ((buf(6) & 255).toLong << 48) +
        ((buf(5) & 255).toLong << 40) +
        ((buf(4) & 255).toLong << 32) +
        ((buf(3) & 255).toLong << 24) +
        ((buf(2) & 255) << 16) +
        ((buf(1) & 255) << 8) +
        (buf(0) & 255)
    }

    def readTimestampz: java.util.Date = {
      val inMicro = readLong
      val millisSinceEpoch = (inMicro / 1000L) + JanFirst2kMillisSinceEpoch
      new java.util.Date(millisSinceEpoch)
    }

    def readDate: java.util.Date = {
      val inDays = readLong
      val millisSinceEpoch = (inDays * 1000L * 3600 * 24) + JanFirst2kMillisSinceEpoch
      new java.util.Date(millisSinceEpoch)
    }

    def readVarchar: String = {
      val len = readInt
      val bytes = readBytes(len)
      val str = new String(bytes, "UTF-8")
      str
    }

    def readChar(colLen: Int): String = {
      val bytes = readBytes(colLen)
      new String(bytes, "UTF-8")
    }

    def readShort: Short = {
      val c2 = s.read
      val c1 = s.read

      // This is the algorithm from DataInputStream
      if ((c1 | c2) < 0) eof else ((c1 << 8) | c2).toShort
    }

    final def skipFully(count: Long): Unit = {
      @annotation.tailrec
      def go(c: Long): Unit = {
        val skipped = s.skip(c)
        if (skipped == c) ()
        else if (skipped == 0L) throw new Exception(s"count, c, skipped = ${(count, c, skipped)}")
        else go(c - skipped)
      }
      if (count != 0L) go(count) else ()
    }
  }

  implicit class RichOutputStream(val s: OutputStream) extends AnyVal {
    def writeBoolean(b: Boolean): Unit = if (b) s.write(1: Byte) else s.write(0: Byte)

    def writeBytes(b: Array[Byte], off: Int, len: Int): Unit = {
      s.write(b, off, len)
    }

    def writeByte(b: Byte): Unit = s.write(b)

    def writeBytes(b: Array[Byte]): Unit = writeBytes(b, 0, b.length)

    def writeDouble(d: Double): Unit = writeLong(java.lang.Double.doubleToLongBits(d))

    def writeFloat(f: Float): Unit = writeInt(java.lang.Float.floatToIntBits(f))

    def writeLong(l: Long): Unit = {
      s.write((l & 0xFF).toByte)
      s.write(((l >>> 8) & 0xFF).toByte)
      s.write(((l >>> 16) & 0xFF).toByte)
      s.write((l >>> 24).toByte)
      s.write((l >>> 32).toByte)
      s.write(((l >>> 40) & 0xFF).toByte)
      s.write(((l >>> 48) & 0xFF).toByte)
      s.write((l >>> 56).toByte)
    }

    def writeInt(i: Int): Unit = {
      s.write((i & 0xFF).toByte)
      s.write(((i >>> 8) & 0xFF).toByte)
      s.write(((i >>> 16) & 0xFF).toByte)
      s.write((i >>> 24).toByte)
    }

    def writeShort(sh: Short): Unit = {
      s.write((sh & 0xFF).toByte)
      s.write(((sh >>> 8) & 0xFF).toByte)
    }

    def writeTimestampz(d: java.util.Date): Unit = {
      val millisSinceJan12k = d.getTime - JanFirst2kMillisSinceEpoch
      val inMicro: Long = millisSinceJan12k * 1000L
      writeLong(inMicro)
    }

    def writeDate(d: java.util.Date): Unit = {
      val millisSinceJan12k = d.getTime - JanFirst2kMillisSinceEpoch
      val inDays: Long = millisSinceJan12k / (1000L * 3600 * 24)
      writeLong(inDays)
    }

    def writeVarchar(str: String): Unit = {
      val bytes = str.getBytes("UTF-8")
      val len = bytes.size
      writeInt(len)
      writeBytes(bytes, 0, len)
    }

    def writeChar(str: String, colLen: Int): Unit = {
      val spaceChar: Byte = " ".getBytes("UTF-8")(0)
      val bytes = str.getBytes("UTF-8")
      val len = bytes.size
      require(len <= colLen, "Attempted to write more bytes than the column allows. Remember bytes are on utf-8 encoded data not characters.")
      writeBytes(bytes, 0, len)
      (0 until (colLen - len)).foreach { _ =>
        writeByte(spaceChar)
      }
    }
  }
}
