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

import java.io.InputStream
import JavaStreamEnrichments._

object PositionInputStream {
  def apply(in: InputStream): PositionInputStream = in match {
    case p: PositionInputStream => p
    case nonPos => new PositionInputStream(nonPos)
  }
}

class PositionInputStream(val wraps: InputStream) extends InputStream {
  private[this] var pos: Long = 0L
  private[this] var markPos: Long = -1L
  def position: Long = pos

  override def available = wraps.available

  override def close(): Unit = { wraps.close() }

  override def mark(limit: Int): Unit = {
    wraps.mark(limit)
    markPos = pos
  }

  override val markSupported: Boolean = wraps.markSupported

  override def read: Int = {
    // returns -1 on eof or 0 to 255 store 1 byte.
    val result = wraps.read
    if (result >= 0) pos += 1
    result
  }
  override def read(bytes: Array[Byte]): Int = {
    val count = wraps.read(bytes)
    // Make this branch true as much as possible to improve branch prediction
    if (count >= 0) pos += count
    count
  }

  override def read(bytes: Array[Byte], off: Int, len: Int): Int = {
    val count = wraps.read(bytes, off, len)
    // Make this branch true as much as possible to improve branch prediction
    if (count >= 0) pos += count
    count
  }

  override def reset(): Unit = {
    wraps.reset()
    pos = markPos
  }

  private def illegal(s: String): Nothing =
    throw new IllegalArgumentException(s)

  override def skip(n: Long): Long = {
    if (n < 0) illegal("Must seek fowards")
    val count = wraps.skip(n)
    // Make this branch true as much as possible to improve branch prediction
    if (count >= 0) pos += count
    count
  }

  /**
   * This throws an exception if it can't set the position to what you give it.
   */
  def seekToPosition(p: Long): Unit = {
    if (p < pos) illegal(s"Can't seek backwards, at position $pos, trying to goto $p")
    wraps.skipFully(p - pos)
    pos = p
  }
}
