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

/**
 * List of Sorted Neighbors backed by an Array
 */
case class SortedNeighbors[T](val neighbors: Array[T]) extends Neighbors[T]
/**
 * Unsorted List of Neighbors backed by an Array
 */
case class UnsortedNeighbors[T](val neighbors: Array[T]) extends Neighbors[T] {
  def toSorted(implicit ord: Ordering[T]): SortedNeighbors[T] = SortedNeighbors(neighbors.sorted)
}

