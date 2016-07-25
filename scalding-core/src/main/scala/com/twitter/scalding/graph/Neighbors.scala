/*
Copyright 2012 Twitter, Inc.

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
package com.twitter.scalding.graph

sealed trait Neighbors[T] {
  def neighbors: Array[T]
}

case class SortedNeighbors[T](val neighbors: Array[T])(implicit ord: Ordering[T]) extends Neighbors[T] {
  def intersectCount(other: SortedNeighbors[T])(implicit ord: Ordering[T]): Int = {
    val (size1, size2) = (neighbors.length, other.neighbors.length)

    val (max, min) = if (size1 > size2) (size1, size2) else (size2, size1)

    if ((size1 + size2) < (min * Math.log(max))) {
      mergeJoin(neighbors, other.neighbors)
    } else {
      if (size1 > size2) {
        binarySearch(neighbors, other.neighbors)
      } else {
        binarySearch(other.neighbors, neighbors)
      }
    }
  }

  /**
   * Compute Intersection by using merge join O(M + N) complexity
   */
  private def mergeJoin(array1: Array[T], array2: Array[T])(implicit ord: Ordering[T]): Int = {
    var (ix1, ix2, intersect) = (0, 0, 0)

    while (ix1 < array1.length && ix2 < array2.length) {
      val comp = ord.compare(array1(ix1), array2(ix2))
      if (comp < 0) {
        ix1 += 1
      } else if (comp > 0) {
        ix2 += 1
      } else {
        intersect += 1

        ix1 += 1
        ix2 += 1
      }
    }

    intersect
  }

  /**
   * Compute Intersection by using binary search O(Mlg(N)) where M is small and N is large
   */
  private def binarySearch(bigArray: Array[T], smallArray: Array[T]): Int = {
    var (ix2, intersect) = (0, 0)

    while (ix2 < smallArray.length) {
      var left = 0
      var right = bigArray.length - 1
      while (left <= right) {
        val mid = left + (right - left) / 2
        val comp = ord.compare(bigArray(mid), smallArray(ix2))
        if (comp == 0) {
          intersect += 1
          left = right + 1
        } else if (comp > 0) {
          right = mid - 1
        } else {
          left = mid + 1
        }
      }

      ix2 += 1
    }

    intersect
  }
}

case class UnsortedNeighbors[T](val neighbors: Array[T]) extends Neighbors[T]

