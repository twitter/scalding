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
    val xor = (a ^ b)
    // If xor >= 0, then a and b are on the same side of zero
    if (xor >= 0L) java.lang.Long.compare(a, b)
    else if (b >= 0L) 1
    else -1
  }
  final def unsignedIntCompare(a: Int, b: Int): Int =
    java.lang.Long.compare(a.toLong & 0xFFFFFFFFL, b.toLong & 0xFFFFFFFFL)

  final def unsignedShortCompare(a: Short, b: Short): Int =
    Integer.compare(a & 0xFFFF, b & 0xFFFF)

  final def unsignedByteCompare(a: Byte, b: Byte): Int =
    Integer.compare(a & 0xFF, b & 0xFF)
}

