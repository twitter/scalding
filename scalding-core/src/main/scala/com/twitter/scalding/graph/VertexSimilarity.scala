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

import com.twitter.scalding.mathematics.SetSimilarity

import scala.reflect.ClassTag

object VertexSimilarity {
  /**
   * Compute the exact cosine between all vertices in the graph.
   */
  def exactCosine[T: Ordering: ClassTag, S, Q](graph: Graph[T, S, Q]): Graph[T, Option[Double], Unit] =
    graph
      .collectNeighborIds(sortNeighbors = true)
      .mapTriplets{
        case (EdgeTriplet(Vertex(id1, nbrs1: SortedNeighbors[T]), Vertex(id2, nbrs2: SortedNeighbors[T]), edge)) =>
          val cosine = SetSimilarity(nbrs1.intersectCount(nbrs2), nbrs1.neighbors.length, nbrs2.neighbors.length).cosine
          EdgeTriplet(Vertex(id1, ()), Vertex(id2, ()), Edge(id1, id2, cosine))
      }
}
