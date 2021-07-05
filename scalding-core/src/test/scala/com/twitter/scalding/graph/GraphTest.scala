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

class GraphTest extends WordSpec with Matchers {
  import TypedPipeChecker._

  val graphPipe = List(
    (4L, 1L),
    (2L, 4L),
    (2L, 1L),
    (5L, 2L),
    (5L, 3L),
    (5L, 6L),
    (3L, 6L),
    (3L, 2L),
    (1L, 2L))

  val vertices = graphPipe.flatMap{ case (s, d) => List(s, d) }.distinct.map(s => Vertex(s, ()))

  implicit val ord = Ordering.by[Vertex[Long, Unit], Long](_.id)

  val edges = graphPipe.map{ case (s, d) => Edge(s, d, ()) }

  "A Graph" should {
    val graph = new Graph(TypedPipe.from(edges), TypedPipe.from(vertices))

    "map vertices" in {
      val updatedGraph = graph.mapVertices{ case (Vertex(id, _)) => id }
      val mapped = TypedPipeChecker.inMemoryToList(updatedGraph.vertices)
      mapped.foreach{ case (Vertex(id, attr)) => assert(id == attr) }
    }

    "map edges" in {
      val updatedGraph = graph.mapEdges{ case (Edge(source, dest, _)) => source }
      val mapped = updatedGraph.edges.inMemoryToList
      mapped.foreach{ case (Edge(source, dest, attr)) => assert(source == attr) }
    }

    "join vertices" in {
      val newVertices = List(Vertex(1L, ()), Vertex(3L, ()))
      val updatedGraph = graph.joinVertices(TypedPipe.from(newVertices)){ case (id, _, _) => id }
      val mapped = updatedGraph.vertices.inMemoryToList
      mapped.foreach{ case (Vertex(id, attr)) => assert(id == attr) }
    }

    "collect edges" in {
      val neighbors = graph.collectEdges.vertices.inMemoryToList

      val vertex = neighbors.find(_.id == 5L)
      assert(vertex.isDefined, "Found the vertex")
      assert(vertex.get.attr.neighbors.map(_.dest) === Array(2L, 3L, 6L))
    }

    "collect neighbors sorted" in {
      val neighbors = graph.collectNeighbors.mapVertices(_.attr.toSorted).vertices.inMemoryToList

      val vertex = neighbors.find(_.id == 2L)
      assert(vertex.isDefined, "Found the vertex")
      assert(vertex.get.attr.neighbors.map(_.id) === Array(1L, 4L))
    }

    "collect neighbors ids sorted" in {
      val neighbors = graph.collectNeighborIds.mapVertices(_.attr.toSorted).vertices.inMemoryToList

      val vertex = neighbors.find(_.id == 2L)
      assert(vertex.isDefined, "Found the vertex")
      assert(vertex.get.attr.neighbors === Array(1L, 4L))
    }

    "subgraph" in {
      def filterEdges(triplet: EdgeTriplet[Long, Unit, Unit]) = triplet.edge.source == 5L
      def vpred(vertex: Vertex[Long, Unit]) = (vertex.id == 5L) || (vertex.id == 2L)

      val subgraph = graph.subgraph(filterEdges, vpred).edges.inMemoryToList

      assert(subgraph.size == 1, "Only one edge")
      assert(subgraph.head == Edge(5L, 2L, ()), "Only one edge")
    }

    "triplets" in {
      val triplets = graph.triplets.inMemoryToList
      val triplet = triplets.find(_.edge.source == 4L)
      assert(triplet.isDefined, "Found the triplet")
      assert(triplet.get === EdgeTriplet(Vertex(4L, ()), Vertex(1L, ()), Edge(4L, 1L, ())))
    }

    "mask" in {
      val filteredGraph = graph.filterEdges(_.source == 5L)
      val masked = graph.mask(filteredGraph).edges.inMemoryToList.sortBy(_.dest)

      val expectedEdges = graphPipe
        .filter{ case (s, _) => s == 5L }
        .sortBy(_._2)
        .map{ case (s, d) => Edge(s, d, ()) }

      assert(masked.size == 3)
      assert(masked === expectedEdges)
    }
  }
}
