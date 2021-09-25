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
package com.twitter.scalding.serialization.macros.impl.ordered_serialization.runtime_helpers

import java.io.InputStream
import scala.collection.mutable.Buffer

object TraversableHelpers {
  import com.twitter.scalding.serialization.JavaStreamEnrichments._

  final def rawCompare(inputStreamA: InputStream, inputStreamB: InputStream)(
    consume: (InputStream, InputStream) => Int): Int = {
    val lenA = inputStreamA.readPosVarInt
    val lenB = inputStreamB.readPosVarInt

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

  final def iteratorCompare[T](iteratorA: Iterator[T], iteratorB: Iterator[T])(
    implicit ord: Ordering[T]): Int = {
    @annotation.tailrec
    def result: Int =
      if (iteratorA.isEmpty) {
        if (iteratorB.isEmpty) 0
        else -1 // a is shorter
      } else {
        if (iteratorB.isEmpty) 1 // a is longer
        else {
          val cmp = ord.compare(iteratorA.next, iteratorB.next)
          if (cmp != 0) cmp
          else result
        }
      }

    result
  }

  final def iteratorEquiv[T](iteratorA: Iterator[T], iteratorB: Iterator[T])(
    implicit eq: Equiv[T]): Boolean = {
    @annotation.tailrec
    def result: Boolean =
      if (iteratorA.isEmpty) iteratorB.isEmpty
      else if (iteratorB.isEmpty) false // not empty != empty
      else eq.equiv(iteratorA.next, iteratorB.next) && result

    result
  }

  /**
   * This returns the same result as
   *
   * implicit val o = ord
   * Ordering[Iterable[T]].compare(travA.toList.sorted, travB.toList.sorted)
   *
   * but it does not do a full sort. Instead it uses a partial quicksort approach
   * the complexity should be O(N + M) rather than O(N log N + M log M) for the full
   * sort case
   */
  final def sortedCompare[T](travA: Iterable[T], travB: Iterable[T])(
    implicit ord: Ordering[T]): Int = {
    def compare(startA: Int, endA: Int, a: Buffer[T], startB: Int, endB: Int, b: Buffer[T]): Int =
      if (startA == endA) {
        if (startB == endB) 0 // both empty
        else -1 // empty is smaller than non-empty
      } else if (startB == endB) 1 // non-empty is bigger than empty
      else {
        @annotation.tailrec
        def partition(pivot: T,
          pivotStart: Int,
          pivotEnd: Int,
          endX: Int,
          x: Buffer[T]): (Int, Int) =
          if (pivotEnd >= endX) (pivotStart, pivotEnd)
          else {
            val t = x(pivotEnd)
            val cmp = ord.compare(t, pivot)
            if (cmp == 0) {
              // the pivot section grows by 1 to include test
              partition(pivot, pivotStart, pivotEnd + 1, endX, x)
            } else if (cmp > 0) {
              // test is bigger, swap it with the end and move the end down:
              val newEnd = endX - 1
              val end = x(newEnd)
              x(newEnd) = t
              x(pivotEnd) = end
              // now try again:
              partition(pivot, pivotStart, pivotEnd, newEnd, x)
            } else {
              // t < pivot so we need to push this value below the pivots:
              val ps = x(pivotStart) // might not be pivot if the pivot size is 0
              x(pivotStart) = t
              x(pivotEnd) = ps
              partition(pivot, pivotStart + 1, pivotEnd + 1, endX, x)
            }
          }
        val pivot = a(startA)
        val (aps, ape) = partition(pivot, startA, startA + 1, endA, a)
        val (bps, bpe) = partition(pivot, startB, startB, endB, b)

        val asublen = aps - startA
        val bsublen = bps - startB
        if (asublen != bsublen) {
          // comparing to the longer is enough
          // because one of them will then include pivots which are larger
          val longer = math.max(asublen, bsublen)
          def extend(s: Int, e: Int) = math.min(s + longer, e)

          if (asublen != 0) {
            /*
             * We can safely recurse because startA does not hold pivot, so we won't
             * do the same algorithm
             */
            compare(startA, extend(startA, endA), a, startB, extend(startB, endB), b)
          } else {
            /*
             * We know that startB does not have the pivot, because if it did, bsublen == 0
             * and both are equal, which is not true in this branch.
             * we can reverse the recursion to ensure we get a different pivot
             */
            -compare(startB, extend(startB, endB), b, startA, extend(startA, endA), a)
          }
        } else {
          // the prefixes are the same size
          val cmp = compare(startA, aps, a, startB, bps, b)
          if (cmp != 0) cmp
          else {
            // we need to look into the pivot area. We don't need to check
            // for equality on the overlapped pivot range
            val apsize = ape - aps
            val bpsize = bpe - bps
            val minpsize = math.min(apsize, bpsize)
            val acheck = aps + minpsize
            val bcheck = bps + minpsize
            if (apsize != bpsize &&
              acheck < endA &&
              bcheck < endB) {
              // exactly one of them has a pivot value
              ord.compare(a(acheck), b(bcheck))
            } else {
              // at least one of them or both is empty, and we pick it up above
              compare(aps + minpsize, endA, a, bps + minpsize, endB, b)
            }
          }
        }
      }

    /**
     * If we are equal unsorted, we are equal.
     * this is useful because often scala will build identical sets
     * exactly the same way, so this fast check will work.
     */
    if (iteratorEquiv(travA.iterator, travB.iterator)(ord)) 0
    else {
      // Let's do the more expensive, potentially full sort, algorithm
      val a = travA.toBuffer
      val b = travB.toBuffer
      compare(0, a.size, a, 0, b.size, b)
    }
  }
}
