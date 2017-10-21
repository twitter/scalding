package com.twitter.scalding.serialization.provided

import com.twitter.scalding.serialization.JavaStreamEnrichments._
import java.nio.ByteBuffer
import java.io.InputStream
import com.twitter.scalding.serialization.Serialization
import com.twitter.scalding.serialization.JavaStreamEnrichments._
import scala.math.min
import scala.util.{ Failure, Try }
import scala.util.control.NonFatal

object OrderedSerializationByteBuffer extends UnsafeOrderedSerialization[ByteBuffer] {

  override def staticSize: Option[Int] = None

  def hash(x: ByteBuffer): Int =
    x.hashCode

  override def unsafeSwitchingCompareBinaryNoConsume(inputStreamA: InputStream, inputStreamB: InputStream, consumeToEnd: Boolean): Int = {
    val lenA = inputStreamA.readPosVarInt
    val lenB = inputStreamB.readPosVarInt
    val queryLength = scala.math.min(lenA, lenB)
    var incr = 0
    var state = 0

    while (incr < queryLength && state == 0) {
      state = java.lang.Byte.compare(inputStreamA.readByte, inputStreamB.readByte)
      incr = incr + 1
    }
    if (consumeToEnd) {
      inputStreamA.skip(lenA - incr)
      inputStreamB.skip(lenB - incr)
    }
    if (state == 0) {
      java.lang.Integer.compare(lenA, lenB)
    } else {
      state
    }
  }

  override def skip(inputStream: InputStream): Try[Unit] = {
    try {
      val lenA = inputStream.readPosVarInt
      inputStream.skip(lenA)
      Serialization.successUnit
    } catch {
      case NonFatal(e) =>
        Failure(e)
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

  def dynamicSize(element: ByteBuffer): Option[Int] = Some {
    val tmpLen = element.remaining
    posVarIntSize(tmpLen) + tmpLen
  }
}
