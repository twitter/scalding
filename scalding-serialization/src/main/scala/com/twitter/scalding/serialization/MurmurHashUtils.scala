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

// Taking a few functions from:
// https://guava-libraries.googlecode.com/git/guava/src/com/google/common/hash/Murmur3_32HashFunction.java
object MurmurHashUtils {
  final val seed = 0xf7ca7fd2
  private final val C1: Int = 0xcc9e2d51
  private final val C2: Int = 0x1b873593

  final def hashInt(input: Int): Int = {
    val k1 = mixK1(input)
    val h1 = mixH1(seed, k1)

    fmix(h1, 4) // length of int is 4 bytes
  }

  final def hashLong(input: Long): Int = {
    val low = input.toInt
    val high = (input >>> 32).toInt

    var k1 = mixK1(low)
    var h1 = mixH1(seed, k1)

    k1 = mixK1(high)
    h1 = mixH1(h1, k1)

    fmix(h1, 8) // 8 bytes
  }

  final def hashUnencodedChars(input: CharSequence): Int = {
    var h1 = seed

    // step through the CharSequence 2 chars at a time
    var i = 0
    while (i < input.length) {
      var k1 = input.charAt(i - 1) | (input.charAt(i) << 16)
      k1 = mixK1(k1)
      h1 = mixH1(h1, k1)
      i += 2
    }

    // deal with any remaining characters
    if ((input.length() & 1) == 1) {
      var k1: Int = input.charAt(input.length() - 1)
      k1 = mixK1(k1)
      h1 ^= k1
    }

    fmix(h1, (Character.SIZE / java.lang.Byte.SIZE) * input.length())
  }

  final def mixK1(k1Input: Int): Int = {
    var k1 = k1Input
    k1 *= C1
    k1 = Integer.rotateLeft(k1, 15)
    k1 *= C2
    k1
  }

  final def mixH1(h1Input: Int, k1Input: Int): Int = {
    var h1 = h1Input
    var k1 = k1Input
    h1 ^= k1
    h1 = Integer.rotateLeft(h1, 13)
    h1 = h1 * 5 + 0xe6546b64
    h1
  }

  // Finalization mix - force all bits of a hash block to avalanche
  final def fmix(h1Input: Int, length: Int): Int = {
    var h1 = h1Input
    h1 ^= length
    h1 ^= h1 >>> 16
    h1 *= 0x85ebca6b
    h1 ^= h1 >>> 13
    h1 *= 0xc2b2ae35
    h1 ^= h1 >>> 16
    h1
  }

  def iteratorHash[T](a: Iterator[T])(hashFn: T => Int): Int = {
    var h1 = seed
    var i = 0
    while (a.hasNext) {
      var k1 = hashFn(a.next)
      h1 = mixH1(h1, k1)
      i += 1
    }
    fmix(h1, i)
  }
}
