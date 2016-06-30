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

import java.io._

object JavaStreamEnrichments {
  def eof: Nothing = throw new EOFException()

  // We use this to avoid allocating a closure to make
  // a lazy parameter to require
  private def illegal(s: String): Nothing =
    throw new IllegalArgumentException(s)

  /**
   * Note this is only recommended for testing.
   * You may want to use ByteArrayInputOutputStream for performance critical concerns
   */
  implicit class RichByteArrayOutputStream(val baos: ByteArrayOutputStream) extends AnyVal {
    def toInputStream: ByteArrayInputStream = new ByteArrayInputStream(baos.toByteArray)
  }

  /**
   * enrichment to treat an Array like an OutputStream
   */
  implicit class RichByteArray(val bytes: Array[Byte]) extends AnyVal {
    def wrapAsOutputStream: ArrayWrappingOutputStream = wrapAsOutputStreamAt(0)
    def wrapAsOutputStreamAt(pos: Int): ArrayWrappingOutputStream =
      new ArrayWrappingOutputStream(bytes, pos)
  }
  /**
   * Wraps an Array so that you can write into it as a stream without reallocations
   * or copying at the end. Useful if you know an upper bound on the number of bytes
   * you will write
   */
  class ArrayWrappingOutputStream(val buffer: Array[Byte], initPos: Int) extends OutputStream {
    if (buffer.length < initPos) {
      illegal(s"Initial position cannot be more than length: $initPos > ${buffer.length}")
    }
    private[this] var pos = initPos
    def position: Int = pos
    override def write(b: Int): Unit = {
      buffer(pos) = b.toByte
      pos += 1
    }
    override def write(b: Array[Byte], off: Int, len: Int): Unit = {
      Array.copy(b, off, buffer, pos, len)
      pos += len
    }
  }

  def posVarIntSize(i: Int): Int = {
    if (i < 0) illegal(s"negative numbers not allowed: $i")
    if (i < ((1 << 8) - 1)) 1
    else {
      if (i < ((1 << 16) - 1)) {
        3
      } else {
        7
      }
    }
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
      if ((c1 | c2) < 0) eof else ((c1 << 8) | c2)
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

    /**
     * This is the algorithm from DataInputStream
     * it was also benchmarked against the approach
     * used in readLong and found to be faster
     */
    def readInt: Int = {
      val c1 = s.read
      val c2 = s.read
      val c3 = s.read
      val c4 = s.read
      if ((c1 | c2 | c3 | c4) < 0) eof else ((c1 << 24) | (c2 << 16) | (c3 << 8) | c4)
    }
    /*
     * This is the algorithm from DataInputStream
     * it was also benchmarked against the same approach used
     * in readInt (buffer-less) and found to be faster.
     */
    def readLong: Long = {
      val buf = new Array[Byte](8)
      readFully(buf)
      (buf(0).toLong << 56) +
        ((buf(1) & 255).toLong << 48) +
        ((buf(2) & 255).toLong << 40) +
        ((buf(3) & 255).toLong << 32) +
        ((buf(4) & 255).toLong << 24) +
        ((buf(5) & 255) << 16) +
        ((buf(6) & 255) << 8) +
        (buf(7) & 255)
    }

    def readChar: Char = {
      val c1 = s.read
      val c2 = s.read
      // This is the algorithm from DataInputStream
      if ((c1 | c2) < 0) eof else ((c1 << 8) | c2).toChar
    }

    def readShort: Short = {
      val c1 = s.read
      val c2 = s.read
      // This is the algorithm from DataInputStream
      if ((c1 | c2) < 0) eof else ((c1 << 8) | c2).toShort
    }

    /**
     * This reads a varInt encoding that only encodes non-negative
     * numbers. It uses:
     * 1 byte for values 0 - 255,
     * 3 bytes for 256 - 65535,
     * 7 bytes for 65536 - Int.MaxValue
     */
    final def readPosVarInt: Int = {
      val c1 = readUnsignedByte
      if (c1 < ((1 << 8) - 1)) c1
      else {
        val c2 = readUnsignedShort
        if (c2 < ((1 << 16) - 1)) c2
        else readInt
      }
    }

    final def skipFully(count: Long): Unit = {
      @annotation.tailrec
      def go(c: Long): Unit = {
        val skipped = s.skip(c)
        if (skipped == c) ()
        else if (skipped == 0L) throw new IOException(s"could not skipFully: count, c, skipped = ${(count, c, skipped)}")
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

    /**
     * This reads a varInt encoding that only encodes non-negative
     * numbers. It uses:
     * 1 byte for values 0 - 255,
     * 3 bytes for 256 - 65535,
     * 7 bytes for 65536 - Int.MaxValue
     */
    def writePosVarInt(i: Int): Unit = {
      if (i < 0) illegal(s"must be non-negative: ${i}")
      if (i < ((1 << 8) - 1)) s.write(i)
      else {
        s.write(-1: Byte)
        if (i < ((1 << 16) - 1)) {
          s.write(i >> 8)
          s.write(i)
        } else {
          // the linter does not like us repeating ourselves here
          s.write(-1)
          s.write(-1) // linter:ignore
          writeInt(i)
        }
      }
    }

    def writeDouble(d: Double): Unit = writeLong(java.lang.Double.doubleToLongBits(d))

    def writeFloat(f: Float): Unit = writeInt(java.lang.Float.floatToIntBits(f))

    def writeLong(l: Long): Unit = {
      s.write((l >>> 56).toInt)
      s.write((l >>> 48).toInt)
      s.write((l >>> 40).toInt)
      s.write((l >>> 32).toInt)
      s.write((l >>> 24).toInt)
      s.write((l >>> 16).toInt)
      s.write((l >>> 8).toInt)
      s.write(l.toInt)
    }

    def writeInt(i: Int): Unit = {
      s.write(i >>> 24)
      s.write(i >>> 16)
      s.write(i >>> 8)
      s.write(i)
    }

    def writeChar(sh: Char): Unit = {
      s.write(sh >>> 8)
      s.write(sh.toInt)
    }

    def writeShort(sh: Short): Unit = {
      s.write(sh >>> 8)
      s.write(sh.toInt)
    }
  }
}
