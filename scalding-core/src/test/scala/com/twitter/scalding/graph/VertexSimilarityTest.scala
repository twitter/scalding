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

import com.twitter.scalding.TypedPipeChecker
import com.twitter.scalding.typed.TypedPipe
import org.scalatest.{ Matchers, WordSpec }

class VertexSimilarityTest extends WordSpec with Matchers {
  val graphPipe = List(
    (4L, 1L),
    (2L, 4L),
    (2L, 1L),
    (5L, 2L),
    (5L, 3L),
    (5L, 6L),
    (3L, 6L),
    (3L, 2L))

  val graph = Graph.fromEdges(TypedPipe.from(graphPipe).map{ case (s, d) => Edge(s, d, ()) })

  "Vertex Similarity" should {
    "compute exact cosine" in {
      val cosine = TypedPipeChecker.inMemoryToList(VertexSimilarity.exactCosine(graph).edges)

      val edge = cosine.find(e => e.source == 2L && e.dest == 4L)

      assert(edge.exists(_.attr.exists(score => math.abs(score - 0.707) < 0.001)))
    }
  }
}