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

import com.twitter.scalding.TypedPipe

import scala.reflect.ClassTag

/**
 * General Graph Object that works on both Vertices and Edges.
 * Graph supports extra data on both edges and vertices.
 *
 * @param inputEdges Directed Edges that make up the graph.
 * @param inputVertices Vertices of the graph.
 */
class Graph[T: Ordering, S, Q](inputEdges: TypedPipe[Edge[T, S]], inputVertices: TypedPipe[Vertex[T, Q]]) {
  def edges: TypedPipe[Edge[T, S]] = inputEdges
  def vertices: TypedPipe[Vertex[T, Q]] = inputVertices

  /**
   * Returns a TypedPipe of edges with joined Vertex attributes.
   */
  def triplets: TypedPipe[EdgeTriplet[T, S, Q]] =
    edges
      .groupBy(_.source)
      .join(vertices.groupBy(_.id))
      .toTypedPipe
      .map{ case (source, (edge, packet)) => (edge.dest, (edge, packet)) }
      .join(vertices.groupBy(_.id))
      .values
      .map{ case ((edge, sourceVertex), destVertex) => EdgeTriplet(sourceVertex, destVertex, edge) }

  /**
   * Left join vertices with the graph vertices and generate a new Graph
   */
  def leftJoinVertices[U, VD2](other: TypedPipe[Vertex[T, U]])(mapFunc: (T, Q, Option[U]) => VD2): Graph[T, S, VD2] = {
    val newVertices = vertices
      .groupBy(_.id)
      .leftJoin(other.groupBy(_.id))
      .toTypedPipe
      .map{ case (id, (vertex, data)) => Vertex(id, mapFunc(id, vertex.attr, data.map(_.attr))) }

    new GraphUnfilteredEdges(edges, newVertices)
  }

  /**
   * Inner join vertices with the graph vertices and generate a new Graph
   */
  def joinVertices[U, VD2](other: TypedPipe[Vertex[T, U]])(mapFunc: (T, Q, U) => VD2): Graph[T, S, VD2] = {
    val newVertices = vertices
      .groupBy(_.id)
      .join(other.groupBy(_.id))
      .toTypedPipe
      .map{ case (id, (vertex, data)) => Vertex(id, mapFunc(id, vertex.attr, data.attr)) }

    new Graph[T, S, VD2](edges, newVertices)
  }

  /**
    * Filtered the vertices of the graph.  Defer the edge filtering until necessary.
    */
  def filterVertices(filter: Vertex[T, Q] => Boolean) =
    new GraphUnfilteredEdges[T, S, Q](edges, vertices.filter(filter))

  /**
    * Filtered the edges of the graph.  Defer the vertex filtering until necessary.
    */
  def filterEdges(filter: Edge[T, S] => Boolean) =
    new GraphUnfilteredVertices[T, S, Q](edges.filter(filter), vertices)
  
  def mapVertices[A](map: Vertex[T, Q] => A): Graph[T, S, A] =
    new Graph[T, S, A](edges, vertices.map{ vertex => Vertex(vertex.id, map(vertex)) })

  def mapEdges[A](map: Edge[T, S] => A): Graph[T, A, Q] =
    new Graph[T, A, Q](edges.map{ edge => edge.copy(attr = map(edge)) }, vertices)

  def mapTriplets[A, B](map: EdgeTriplet[T, S, Q] => EdgeTriplet[T, A, B]): Graph[T, A, B] = {
    val newTriplets = triplets.map(map)

    new Graph[T, A, B](
      newTriplets.map(_.edge),
      newTriplets.flatMap(trip => List(trip.source, trip.dest)).distinct(Ordering.by(_.id)))
  }

  /**
   * For all vertices collect their neighbors storing only ids.
   */
  def collectNeighborIds(implicit ct: ClassTag[T]): Graph[T, S, UnsortedNeighbors[T]] = {
    val nbrs = edges
      .map{ edge => (edge.source, edge.dest) }
      .group
      .mapGroup{
        case (vert, neighbors) =>
          Iterator.single(Vertex(vert, UnsortedNeighbors(neighbors.toArray)))
      }
      .values

    Graph(edges, nbrs)
  }

  /**
   * For all vertices collect their neighbors.
   */
  def collectNeighbors(implicit ct: ClassTag[T]): Graph[T, S, UnsortedNeighbors[Vertex[T, Q]]] = {
    val nbrs = edges
      .map{ edge => (edge.dest, edge.source) }
      .join(vertices.groupBy(_.id))
      .toTypedPipe
      .map{ case (dest, (source, vertex)) => (source, (dest, vertex)) }
      .group
      .mapGroup{
        case (id, vertexes) =>
          Iterator.single(Vertex(id, UnsortedNeighbors(vertexes.toArray.map(_._2))))
      }
      .values

    Graph(edges, nbrs)
  }

  /**
   * Returns each Vertex with all out going edges.
   * Optionally sort the edges
   */
  def collectEdges(implicit ct: ClassTag[T]): Graph[T, S, UnsortedNeighbors[Edge[T, S]]] = {
    val vertices = edges
      .map{ edge => (edge.source, edge) }
      .group
      .mapGroup{
        case (id, edgeList) =>
          Iterator.single(Vertex(id, UnsortedNeighbors(edgeList.toArray.sortBy(_.dest))))
      }
      .values

    Graph(edges, vertices)
  }

  /**
   * Filter the graph by the Edge and Vertex filters.
   */
  def subgraph(epred: EdgeTriplet[T, S, Q] => Boolean = _ => true, vpred: Vertex[T, Q] => Boolean = _ => true): Graph[T, S, Q] = {
    val newTriplets = triplets.filter(epred)

    new GraphUnfilteredEdges[T, S, Q](
      newTriplets.map(_.edge),
      newTriplets
        .flatMap(trip => List(trip.source, trip.dest))
        .filter(vpred)
        .distinct(Ordering.by(_.id)))
  }

  /**
   * The current graph is filtered to only include the edges and vertices from the other graph.
   * The attribute of the other graph does not matter, the current attributes are kept.
   */
  def mask[A, B](other: Graph[T, A, B]): Graph[T, S, Q] = {
    val fEdges = edges
      .map{ e => ((e.dest, e.source), e) }
      .group
      .join(other.edges.map{ e => ((e.dest, e.source), ()) }.group)
      .toTypedPipe
      .map{ case (_, (e, _)) => e }

    val fVertices = vertices
      .groupBy(_.id)
      .join(other.vertices.groupBy(_.id))
      .toTypedPipe
      .map{ case (_, (v, _)) => v }

    new Graph[T, S, Q](fEdges, fVertices)
  }
}

/**
 * Sometimes working just on deges is required, in those cases we don't also want to
 * take the computational hit of filtering vertices by the updated edges.  In those cases
 * you can return this subgraph that will only filter the vertices when necessary
 */
class GraphUnfilteredVertices[T: Ordering, S, Q](inputEdges: TypedPipe[Edge[T, S]], inputVertices: TypedPipe[Vertex[T, Q]])
  extends Graph[T, S, Q](inputEdges, inputVertices) {
  override def vertices = {
    val graphVertices = edges.flatMap(e => List(e.source, e.dest)).distinct
    inputVertices
      .groupBy(_.id)
      .join(graphVertices.asKeys)
      .values
      .map(_._1)
  }
}

/**
 * Sometimes working just on vertices is required, in those cases we don't also want to
 * take the computational hit of filtering edges by the updated vertices.  In those cases
 * you can return this subgraph that will only filter the edges when necessary
 */
class GraphUnfilteredEdges[T: Ordering, S, Q](inputEdges: TypedPipe[Edge[T, S]], inputVertices: TypedPipe[Vertex[T, Q]])
  extends Graph[T, S, Q](inputEdges, inputVertices) {

  override def edges =
    inputEdges
      .groupBy(_.source)
      .join(vertices.map(_.id).asKeys)
      .values
      .map(_._1)
      .groupBy(_.dest)
      .join(vertices.map(_.id).asKeys)
      .values
      .map(_._1)

}

object Graph {
  /**
   * Generate a graph from a set of Directed Edges.
   */
  def fromEdges[T: Ordering, S](edges: TypedPipe[Edge[T, S]]): Graph[T, S, Unit] = {
    val vertices = edges
      .flatMap(e => List(e.source, e.dest))
      .distinct
      .map(vertex => Vertex(vertex, ()))

    new Graph(edges, vertices)
  }

  /**
   * Generate a graph from a set of Directed Edges and Vertices
   */
  def apply[T: Ordering, S, Q](edges: TypedPipe[Edge[T, S]], vertices: TypedPipe[Vertex[T, Q]]): Graph[T, S, Q] =
    new Graph(edges, vertices)
}
