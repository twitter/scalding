package com.twitter.scalding.dagon

import scala.collection.mutable

object Graphs {

  /**
   * Return the depth first enumeration of reachable nodes, NOT INCLUDING INPUT, unless it can be reached via
   * neighbors
   */
  def depthFirstOf[T](t: T)(nf: NeighborFn[T]): List[T] =
    reflexiveTransitiveClosure(nf(t).toList)(nf)

  /**
   * All the nodes we can reach from this start, including the initial nodes
   */
  def reflexiveTransitiveClosure[T](start: List[T])(nf: NeighborFn[T]): List[T] = {
    @annotation.tailrec
    def loop(stack: List[T], deps: List[T], acc: Set[T]): List[T] =
      stack match {
        case Nil => deps
        case h :: tail =>
          val newStack = nf(h).filterNot(acc).foldLeft(tail) { (s, it) =>
            it :: s
          }
          val newDeps = if (acc(h)) deps else h :: deps
          loop(newStack, newDeps, acc + h)
      }

    loop(start, start.distinct, start.toSet).reverse
  }

  /**
   * Return a NeighborFn for the graph of reversed edges defined by this set of nodes and nf We avoid Sets
   * which use hash-codes which may depend on addresses which are not stable from one run to the next.
   */
  def reversed[T](nodes: Iterable[T])(nf: NeighborFn[T]): NeighborFn[T] = {
    val graph: Map[T, List[T]] = nodes
      .foldLeft(Map.empty[T, List[T]]) { (g, child) =>
        val gWithChild = g + (child -> g.getOrElse(child, Nil))
        nf(child).foldLeft(gWithChild) { (innerg, parent) =>
          innerg + (parent -> (child :: innerg.getOrElse(parent, Nil)))
        }
      }
      // make sure the values are sets, not .mapValues is lazy in scala
      .map { case (k, v) => (k, v.distinct) }

    graph.getOrElse(_, Nil)
  }

  /**
   * Return the depth of each node in the dag. a node that has no dependencies has depth == 0 else it is max
   * of parent + 1
   *
   * Behavior is not defined if the graph is not a DAG (for now, it runs forever, may throw later)
   */
  def dagDepth[T](nodes: Iterable[T])(nf: NeighborFn[T]): Map[T, Int] = {
    val acc = mutable.Map.empty[T, Int]

    @annotation.tailrec
    def computeDepth(todo: Set[T]): Unit =
      if (!todo.isEmpty) {
        def withParents(n: T) = (n :: (nf(n).toList)).filterNot(acc.contains(_)).distinct

        val (doneThisStep, rest) = todo
          .map {
            withParents(_)
          }
          .partition {
            _.size == 1
          }

        acc ++= doneThisStep.flatten.map { n =>
          val depth = nf(n) // n is done now, so all it's neighbors must be too.
            .map {
              acc(_) + 1
            }
            .reduceOption {
              _ max _
            }
            .getOrElse(0)
          n -> depth
        }
        computeDepth(rest.flatten)
      }

    computeDepth(nodes.toSet)
    acc.toMap
  }
}
