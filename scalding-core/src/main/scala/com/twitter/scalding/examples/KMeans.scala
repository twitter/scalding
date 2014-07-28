package com.twitter.scalding.examples

import com.twitter.scalding._
import com.twitter.scalding.typed.ComputedValue

object KMeans {

  /**
   * This is the euclidean norm between two vectors
   */
  private def distance(v1: Vector[Double], v2: Vector[Double]): Double =
    math.sqrt(v1.iterator
      .zip(v2.iterator)
      .map { case (l, r) => (l - r) * (l - r) }
      .sum)

  // Just normal vector addition
  private def add(v1: Vector[Double], v2: Vector[Double]): Vector[Double] =
    v1.zip(v2).map { case (l, r) => l + r }

  // normal scalar multiplication
  private def scale(s: Double, v: Vector[Double]): Vector[Double] =
    v.map { x => s * x }

  // Here we return the centroid of some vectors
  private def centroidOf(vecs: TraversableOnce[Vector[Double]]): Vector[Double] = {
    val (vec, count) = vecs
      // add a 1 to each value to count the number of vectors in one pass:
      .map { v => (v, 1) }
      // Here we add both the count and the vectors:
      .reduce { (ll, rr) =>
        val (l, lc) = ll
        val (r, rc) = rr
        (add(l, r), lc + rc)
      }
    // Now scale to get the pointwise average
    scale(1.0 / count, vec)
  }

  private def closest[Id](from: Vector[Double],
    centroids: TraversableOnce[(Id, Vector[Double])]): (Id, Vector[Double]) =
    centroids
      // compute the distance to each center
      .map { case (id, cent) => (distance(from, cent), (id, cent)) }
      // take the minimum by the distance, ignoring the id and the centroid
      .minBy { case (dist, _) => dist }
      // Just keep the id and the centroid
      ._2

  type LabeledVector = (Int, Vector[Double])

  /**
   * This runs one step in a kmeans algorithm
   * It returns the number of vectors that changed clusters,
   * the new clusters
   * and the new list of labeled vectors
   */
  def kmeansStep(k: Int,
    clusters: ValuePipe[List[LabeledVector]],
    points: TypedPipe[LabeledVector]): Execution[(Long, ValuePipe[List[LabeledVector]], TypedPipe[LabeledVector])] = {

    // Do a cross product to produce all point, cluster pairs
    // in scalding, the smaller pipe should go on the right.
    val next = points.leftCross(clusters)
      // now compute the closest cluster for each vector
      .map {
        // we only handle the case were the cluster
        case ((oldId, vector), Some(centroids)) =>
          val (id, newcentroid) = closest(vector, centroids)
          (id, vector, oldId)
        case (_, None) => sys.error("Missing clusters, this should never happen")
      }
      .forceToDiskExecution

    // How many vectors changed?
    val changedVectors: Execution[Long] = next.flatMap { pipe =>
      pipe
        // Collect only returns a value if the case is matched
        .collect { case (newId, _, oldId) if (newId != oldId) => 1L }
        // sum on a pipe adds everything in that pipe
        .sum
        .toOptionExecution
        // If there were no values, it is because no one changed
        .map(_.getOrElse(0L))
    }

    // Now update the clusters:
    val nextCluster: Execution[ValuePipe[List[LabeledVector]]] = next.map { pipe =>
      ComputedValue(pipe
        .map { case (newId, vector, oldId) => (newId, vector) }
        .group
        // There is no need to use more than k reducers
        .withReducers(k)
        .mapValueStream { vectors => Iterator(centroidOf(vectors)) }
        // Now collect them all into one big
        .groupAll
        .toList
        // discard the "all" key used to group them together
        .values)
    }

    val nextVectors = next.map { pipe => pipe.map { case (newId, vector, _) => (newId, vector) } }
    Execution.zip(changedVectors, nextCluster, nextVectors)
  }

  def initializeClusters(k: Int, points: TypedPipe[Vector[Double]]): (ValuePipe[List[LabeledVector]], TypedPipe[LabeledVector]) = {
    val rng = new java.util.Random(123)
    // take a random k vectors:
    val clusters = points.map { v => (rng.nextDouble, v) }
      .groupAll
      .sortedTake(k)(Ordering.by(_._1))
      .mapValues { randk =>
        randk.iterator
          .zipWithIndex
          .map { case ((_, v), id) => (id, v) }
          .toList
      }
      .values

    // attach a random cluster to each vector
    val labeled = points.map { v => (rng.nextInt(k), v) }

    (ComputedValue(clusters), labeled)
  }

  /*
   * Run the full k-means algorithm by flatMapping the above function into itself
   * while the number of vectors that changed is not zero
   */
  def kmeans(k: Int,
    clusters: ValuePipe[List[LabeledVector]],
    points: TypedPipe[LabeledVector]): Execution[(Int, ValuePipe[List[LabeledVector]], TypedPipe[LabeledVector])] = {

    def go(c: ValuePipe[List[LabeledVector]],
      p: TypedPipe[LabeledVector],
      step: Int): Execution[(Int, ValuePipe[List[LabeledVector]], TypedPipe[LabeledVector])] =

      kmeansStep(k, c, p).flatMap {
        case (changed, nextC, nextP) =>
          if (changed == 0L) Execution.from((step, nextC, nextP))
          else go(nextC, nextP, step + 1)
      }

    go(clusters, points, 0)
  }

  def apply(k: Int, points: TypedPipe[Vector[Double]]): Execution[(Int, ValuePipe[List[LabeledVector]], TypedPipe[LabeledVector])] = {
    val (clusters, labeled) = initializeClusters(k, points)
    kmeans(k, clusters, labeled)
  }
}
