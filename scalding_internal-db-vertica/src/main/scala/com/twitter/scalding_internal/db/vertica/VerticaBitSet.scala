package com.twitter.scalding_internal.db.vertica

import com.twitter.scalding_internal.db._
import scala.util.{ Try, Success, Failure }
import java.io._

object VerticaBitSet {
  def apply(requestedSize: Int): VerticaBitSet = {
    val storage: Array[Byte] = new Array((requestedSize + 7) / 8)
    VerticaBitSet(requestedSize, storage)
  }
}

case class VerticaBitSet(requestedSize: Int, storage: Array[Byte]) extends java.io.Serializable {

  def setNull(col: Int) = set(col, true)
  def setNotNull(col: Int) = set(col, false)

  def set(col: Int, value: Boolean) {
    val bit = if (value) 1 else 0

    val whichIndex = col / 8
    val subIndex = col % 8

    val bitToSet: Byte = (bit << (7 - subIndex)).toByte
    val bitMask: Byte = (~(1 << (7 - subIndex)).toByte).toByte

    val oldValue = storage(whichIndex)
    // zero the field first:
    val maskedValue = (oldValue & bitMask).toByte

    // now and in the field we want
    storage(whichIndex) = (maskedValue | bitToSet).toByte
  }

  def byteToString(b: Byte): String = {
    val masks = Array[Byte](-128, 64, 32, 16, 8, 4, 2, 1)
    val builder = new StringBuilder()
    masks.map { m =>
      if ((b & m) == m) {
        builder.append('1')
      } else {
        builder.append('0')
      }
    }
    builder.toString()
  }

  override def toString = {
    storage.map { b =>
      byteToString(b)
    }.mkString("")
  }

  def getArray = storage
}
