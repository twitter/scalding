package com.twitter.scalding.serialization

import java.io.{ InputStream, OutputStream, EOFException }

object JavaStreamEnrichments {

  /**
   * This has a lot of methods from DataInputStream without
   * having to allocate to get them
   * This code is similar to those algorithms
   */
  implicit class RichInputStream(val s: InputStream) extends AnyVal {
    def eof: Nothing = throw new EOFException()
    def readUnsignedByte: Int = {
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
      def go(o: Int, l: Int): Unit = {
        val count = s.read(bytes, o, l)
        if (count < 0) eof
        else if (count == l) ()
        else go(o + count, l - count)
      }
      go(offset, len)
    }

    def readDouble: Double = java.lang.Double.longBitsToDouble(readLong)
    def readFloat: Float = java.lang.Float.intBitsToFloat(readInt)

    def readInt: Int = {
      val c1 = s.read
      val c2 = s.read
      val c3 = s.read
      val c4 = s.read
      // This is the algorithm from DataInputStream
      if ((c1 | c2 | c3 | c4) < 0) eof else ((c1 << 24) | (c2 << 16) | (c3 << 8) | c4)
    }
    def readLong: Long = {
      // This is the algorithm from DataInputStream
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
    /**
     * This reads a varInt encoding that only encodes non-negative
     * numbers. It uses:
     * 1 byte for values 0 - 255,
     * 3 bytes for 256 - 65535,
     * 7 bytes for 65536 - Int.MaxValue
     */
    final def readSize: Int = {
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
        else go(c - skipped)
      }
      go(count)
    }
  }

  implicit class RichOutputStream(val s: OutputStream) extends AnyVal {
    /**
     * This reads a varInt encoding that only encodes non-negative
     * numbers. It uses:
     * 1 byte for values 0 - 255,
     * 3 bytes for 256 - 65535,
     * 7 bytes for 65536 - Int.MaxValue
     */
    def writeSize(i: Int): Unit = {
      require(i >= 0, s"sizes must be non-negative: ${i}")
      if (i < ((1 << 8) - 1)) s.write(i.toByte)
      else {
        if (i < ((1 << 16) - 1)) {
          val b1 = (i >> 8).toByte
          val b2 = (i & 0xFF).toByte
          s.write(b1)
          s.write(b2)
        } else writeInt(i)
      }
    }

    def writeInt(i: Int): Unit = {
      s.write((i >>> 24).toByte)
      s.write(((i >>> 16) & 0xFF).toByte)
      s.write(((i >>> 8) & 0xFF).toByte)
      s.write((i & 0xFF).toByte)
    }
  }
}
