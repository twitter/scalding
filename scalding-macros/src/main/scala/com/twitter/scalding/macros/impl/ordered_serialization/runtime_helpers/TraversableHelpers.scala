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

    val minLen = math.min(lenA, lenB)
    var incr = 0
    var curIncr = 0
    while (incr < minLen && curIncr == 0) {
      curIncr = consume(inputStreamA, inputStreamB)
      incr = incr + 1
    }

    if (curIncr != 0) curIncr
    else java.lang.Integer.compare(lenA, lenB)
  }

  final def iteratorCompare[T](iteratorA: Iterator[T], iteratorB: Iterator[T], ord: Ordering[T]): Int = {
    @annotation.tailrec
    def result: Int =
      if (iteratorA.isEmpty) {
        if (iteratorB.isEmpty) 0
        else -1 // a is shorter
      }
      else {
        if (iteratorB.isEmpty) 1 // a is longer
        else {
          val cmp = ord.compare(iteratorA.next, iteratorB.next)
          if (cmp != 0) cmp
          else result
        }
      }

    result
  }

  // TODO: don't need to do a total sort here.
  // we could instead do a modified quicksort: if one is empty, we are done
  // if both are not empty, take the first from the left and find all the items <= that
  // do the same on right. call this method again on this partition. If it is equal then try
  // again on the tail
  final def memCompareWithSort[T: ClassTag](travA: Iterable[T], travB: Iterable[T], ord: Ordering[T]): Int = {
    def toSorted(i: Iterable[T]): Array[T] = {
      val array = new Array[T](i.size)
      var pos = 0
      i.foreach { a =>
        array(pos) = a
        pos += 1
      }
      scala.util.Sorting.quickSort(array)(ord)
      array
    }
    iteratorCompare(toSorted(travA).iterator, toSorted(travB).iterator, ord)
  }
}
