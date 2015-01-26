/*
 Copyright 2014 Twitter, Inc.

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
package com.twitter.scalding.macros.impl.ordered_serialization.runtime_helpers

import java.io.InputStream
import scala.reflect.ClassTag

object TraversableHelpers {
  import com.twitter.scalding.serialization.JavaStreamEnrichments._

  final def rawCompare(inputStreamA: InputStream, inputStreamB: InputStream)(consume: (InputStream, InputStream) => Int): Int = {
    val lenA = inputStreamA.readSize
    val lenB = inputStreamB.readSize

    val minLen = _root_.scala.math.min(lenA, lenB)
    var incr = 0
    var curIncr = 0
    while (incr < minLen && curIncr == 0) {
      curIncr = consume(inputStreamA, inputStreamB)
      incr = incr + 1
    }

    if (curIncr != 0) {
      curIncr
    } else {
      if (lenA < lenB) {
        -1
      } else if (lenA > lenB) {
        1
      } else {
        0
      }
    }
  }

  final def sharedMemCompare[T](iteratorA: Iterator[T], lenA: Int, iteratorB: Iterator[T], lenB: Int)(cmp: (T, T) => Int): Int = {
    val minLen: Int = _root_.scala.math.min(lenA, lenB)
    var incr: Int = 0
    var curIncr: Int = 0
    while (incr < minLen && curIncr == 0) {
      curIncr = cmp(iteratorA.next, iteratorB.next)
      incr = incr + 1
    }

    if (curIncr != 0) {
      curIncr
    } else {
      if (lenA < lenB) {
        -1
      } else if (lenA > lenB) {
        1
      } else {
        0
      }
    }
  }

  final def memCompareWithSort[T: ClassTag](travA: TraversableOnce[T], travB: TraversableOnce[T])(compare: (T, T) => Int): Int = {
    val iteratorA: Iterator[T] = travA.toArray.sortWith { (a: T, b: T) =>
      compare(a, b) < 0
    }.toIterator

    val iteratorB: Iterator[T] = travB.toArray.sortWith { (a: T, b: T) =>
      compare(a, b) < 0
    }.toIterator

    val lenA = travA.size
    val lenB = travB.size
    sharedMemCompare(iteratorA, lenA, iteratorB, lenB)(compare)
  }

  final def memCompare[T: ClassTag](travA: TraversableOnce[T], travB: TraversableOnce[T])(compare: (T, T) => Int): Int = {
    val lenA = travA.size
    val lenB = travB.size
    sharedMemCompare(travA.toIterator, lenA, travB.toIterator, lenB)(compare)
  }
}