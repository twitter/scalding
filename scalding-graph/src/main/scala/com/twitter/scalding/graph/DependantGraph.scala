/*
 Copyright 2013 Twitter, Inc.

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

/**
 * Given Dag and a List of immutable nodes, and a function to get
 * dependencies, compute the dependants (reverse the graph)
 */
abstract class DependantGraph[T] {
  def nodes: List[T]
  def dependenciesOf(t: T): Iterable[T]

  lazy val allTails: List[T] = nodes.filter { fanOut(_).get == 0 }
  private lazy val nodeSet: Set[T] = nodes.toSet

  /**
   * This is the dependants graph. Each node knows who it depends on
   * but not who depends on it without doing this computation
   */
  private lazy val graph: NeighborFn[T] = reversed(nodes)(dependenciesOf(_))

  private lazy val depths: Map[T, Int] = dagDepth(nodes)(dependenciesOf(_))

  /**
   * The max of zero and 1 + depth of all parents if the node is the graph
   */
  def isNode(p: T): Boolean = nodeSet.contains(p)
  def depth(p: T): Option[Int] = depths.get(p)

  def dependantsOf(p: T): Option[List[T]] =
    if (isNode(p)) Some(graph(p).toList) else None

  def fanOut(p: T): Option[Int] = dependantsOf(p).map { _.size }
  /**
   * Return all dependendants of a given node.
   * Does not include itself
   */
  def transitiveDependantsOf(p: T): List[T] = depthFirstOf(p)(graph)
}
