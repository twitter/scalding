package com.twitter.scalding.examples

import com.twitter.scalding._
import com.twitter.scalding.mathematics.{ Matrix, ColVector }
import com.twitter.scalding.mathematics.Matrix._

/**
 * A weighted PageRank implementation using the Scalding Matrix API. This
 * assumes that all rows and columns are of type {@link Int} and values or egde
 * weights are {@link Double}. If you want an unweighted PageRank, simply set
 * the weights on the edges to 1.
 *
 * Input arguments:
 *
 *  d -- damping factor
 *  n -- number of nodes in the graph
 *  currentIteration -- start with 0 probably
 *  maxIterations -- stop after n iterations
 *  convergenceThreshold -- using the sum of the absolute difference between
 *                          iteration solutions, iterating stops once we reach
 *                          this threshold
 *  rootDir -- the root directory holding all starting, intermediate and final
 *             data/output
 *
 * The expected structure of the rootDir is:
 *
 *   rootDir
 *     |- iterations
 *     |  |- 0       <-- a TSV of (row, value) of size n, value can be 1/n (generate this)
 *     |  |- n       <-- holds future iterations/solutions
 *     |- edges      <-- a TSV of (row, column, value) for edges in the graph
 *     |- onesVector <-- a TSV of (row, 1) of size n (generate this)
 *     |- diff       <-- a single line representing the difference between the last iterations
 *     |- constants  <-- built at iteration 0, these are constant for any given matrix/graph
 *        |- M_hat
 *        |- priorVector
 *
 * Don't forget to set the number of reducers for this job:
 * -D mapred.reduce.tasks=n
 */
class WeightedPageRankFromMatrix(args: Args) extends Job(args) {

  val d = args("d").toDouble // aka damping factor
  val n = args("n").toInt // number of nodes in the graph

  val currentIteration = args("currentIteration").toInt
  val maxIterations = args("maxIterations").toInt
  val convergenceThreshold = args("convergenceThreshold").toDouble

  val rootDir = args("rootDir")
  val edgesLoc = rootDir + "/edges"
  val onesVectorLoc = rootDir + "/onesVector"

  val iterationsDir = rootDir + "/iterations"
  val previousVectorLoc = iterationsDir + "/" + currentIteration
  val nextVectorLoc = iterationsDir + "/" + (currentIteration + 1)

  val diffLoc = rootDir + "/diff"

  // load the previous iteration
  val previousVector = colVectorFromTsv(previousVectorLoc)

  // iterate, write results
  // R(t + 1) = d * M * R(t) + ((1 - d) / n) * _1_
  val nextVector = M_hat * previousVector + priorVector
  nextVector.write(Tsv(nextVectorLoc))

  measureConvergenceAndStore()

  /**
   * Recurse and iterate again iff we are under the max number of iterations and
   * vector has not converged.
   */
  override def next = {
    val diff = TypedTsv[Double](diffLoc).toIterator.next

    if (currentIteration + 1 < maxIterations && diff > convergenceThreshold) {
      val newArgs = args + ("currentIteration", Some((currentIteration + 1).toString))
      Some(clone(newArgs))
    } else {
      None
    }
  }

  /**
   * Measure convergence by  calculating the total of the absolute difference
   * between the previous and next vectors. This stores the result after
   * calculation.
   */
  def measureConvergenceAndStore(): Unit = {
    (previousVector - nextVector).
      mapWithIndex { case (value, index) => math.abs(value) }.
      sum.
      write(TypedTsv[Double](diffLoc))
  }

  /**
   * Load or generate on first iteration the matrix M^ given A.
   */
  def M_hat: Matrix[Int, Int, Double] = {

    if (currentIteration == 0) {
      val A = matrixFromTsv(edgesLoc)
      val M = A.rowL1Normalize.transpose
      val M_hat = d * M

      M_hat.write(Tsv(rootDir + "/constants/M_hat"))
    } else {
      matrixFromTsv(rootDir + "/constants/M_hat")
    }
  }

  /**
   * Load or generate on first iteration the prior vector given d and n.
   */
  def priorVector: ColVector[Int, Double] = {

    if (currentIteration == 0) {
      val onesVector = colVectorFromTsv(onesVectorLoc)
      val priorVector = ((1 - d) / n) * onesVector.toMatrix(0)

      priorVector.getCol(0).write(Tsv(rootDir + "/constants/priorVector"))
    } else {
      colVectorFromTsv(rootDir + "/constants/priorVector")
    }
  }

  def matrixFromTsv(input: String): Matrix[Int, Int, Double] =
    TypedTsv[(Int, Int, Double)](input).toMatrix

  def colVectorFromTsv(input: String): ColVector[Int, Double] =
    TypedTsv[(Int, Double)](input).toCol
}
