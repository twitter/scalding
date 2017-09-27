package com.twitter.scalding.serialization.provided

import com.twitter.scalding.serialization.JavaStreamEnrichments._
import java.nio.ByteBuffer
import java.io.InputStream
import com.twitter.scalding.serialization.ComplexHelper

object OrderedSerializationByteBuffer extends ComplexHelper[ByteBuffer] {
  def hash(x: ByteBuffer): Int =
    x.hashCode

  def unsafeCompareBinary(inputStreamA: InputStream, inputStreamB: InputStream): Int = {
    val lenA = inputStreamA.readPosVarInt
    val lenB = inputStreamB.readPosVarInt
    val queryLength = _root_.scala.math.min(lenA, lenB)
    var incr = 0
    var state = 0

    while (incr < queryLength && state == 0) {
      state = java.lang.Byte.compare(inputStreamA.readByte, inputStreamB.readByte)
      incr = incr + 1
    }
    if (state == 0) {
      java.lang.Integer.compare(lenA, lenB)
    } else {
      state
    }
  }

  def unsafeWrite(outputStream: java.io.OutputStream, element: ByteBuffer): Unit = {
    outputStream.writePosVarInt(element.remaining)
    outputStream
      .writeBytes(element.array, element.arrayOffset + element.position, element.remaining)
  }

  def unsafeRead(inputStream: java.io.InputStream): ByteBuffer = {
    val lenA = inputStream.readPosVarInt
    val bytes = new Array[Byte](lenA)
    inputStream.readFully(bytes)
    java.nio.ByteBuffer.wrap(bytes)
  }

  def compare(a: ByteBuffer, b: ByteBuffer): Int = a.compareTo(b)

  def dynamicSizeWithoutLen(element: ByteBuffer): Option[Int] = Some {
    val tmpLen = element.remaining
    posVarIntSize(tmpLen) + tmpLen
  }
}
