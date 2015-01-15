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

object UnsignedComparisons {
  final def unsignedLongCompare(a: Long, b: Long): Int = if (a == b) 0 else {
    // We only get into this block when the a != b, so it has to be the last
    // block
    val firstBitXor = (a ^ b) & (1L << 63)
    // If both are on the same side of zero, normal compare works
    if (firstBitXor == 0) java.lang.Long.compare(a, b)
    else if (b >= 0) 1
    else -1
  }
  final def unsignedIntCompare(a: Int, b: Int): Int = if (a == b) 0 else {
    val firstBitXor = (a ^ b) & (1 << 31)
    // If both are on the same side of zero, normal compare works
    if (firstBitXor == 0) Integer.compare(a, b)
    else if (b >= 0) 1
    else -1
  }
  final def unsignedShortCompare(a: Short, b: Short): Int = if (a == b) 0 else {
    // We have to convert to bytes to Int on JVM to do
    // anything anyway, so might as well compare in that space
    def fromShort(x: Short): Int = if (x < 0) x + (1 << 16) else x.toInt
    Integer.compare(fromShort(a), fromShort(b))
  }
  final def unsignedByteCompare(a: Byte, b: Byte): Int = if (a == b) 0 else {
    // We have to convert to bytes to Int on JVM to do
    // anything anyway, so might as well compare in that space
    def fromByte(x: Byte): Int = if (x < 0) x + (1 << 8) else x.toInt
    Integer.compare(fromByte(a), fromByte(b))
  }
}

