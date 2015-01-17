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
  override def close() { wraps.close() }
  override def mark(limit: Int) {
    wraps.mark(limit)
    markPos = pos
  }
  override def markSupported: Boolean = wraps.markSupported
  override def read: Int = {
    val count = wraps.read
    if (count > 0) pos += 1
    count
  }
  override def read(bytes: Array[Byte]): Int = read(bytes, 0, bytes.length)

  override def read(bytes: Array[Byte], off: Int, len: Int): Int = {
    val count = wraps.read(bytes, off, len)
    if (count > 0) pos += count
    count
  }
  override def reset() {
    wraps.reset()
    pos = markPos
  }
  override def skip(n: Long): Long = {
    val count = skip(n)
    pos += count
    count
  }

  /**
   * This throws an exception if it can't set the position to what you give it.
   */
  def seekToPosition(p: Long) {
    wraps.skipFully(p - pos)
    pos = p
  }
}
