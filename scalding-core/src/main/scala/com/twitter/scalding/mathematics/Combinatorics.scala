package com.twitter.scalding.mathematics
import com.twitter.scalding._
import com.twitter.scalding.Dsl._
import cascading.flow.FlowDef
import cascading.tuple.{ Fields, TupleEntry }
import cascading.pipe.Pipe

/**
 * Serve as a repo for self-contained combinatorial functions with no dependencies
 * such as
 * combinations, aka n choose k, nCk
 * permutations , aka nPk
 * subset sum : numbers that add up to a finite sum
 * weightedSum: For weights (a,b,c, ...), want integers (x,y,z,...) to satisfy constraint  |ax + by + cz + ... - result | < error
 * ...
 *
 * @author : Krishnan Raman, kraman@twitter.com
 */

object Combinatorics {

  /**
   * Given an int k, and an input of size n,
   * return a pipe with nCk combinations, with k columns per row
   *
   *
   * Computes nCk = n choose k, for large values of nCk
   *
   * Use-case: Say you have 100 hashtags sitting in an array
   * You want a table with 5 hashtags per row, all possible combinations
   * If the hashtags are sitting in a string array, then
   * combinations[String]( hashtags, 5)
   * will create the 100 chose 5 combinations.
   *
   * Algorithm: Use k pipes, cross pipes two at a time, filter out non-monotonic entries
   *
   * eg. 10C2 = 10 choose 2
   * Use 2 pipes.
   * Pipe1 = (1,2,3,...10)
   * Pipe2 = (2,3,4....10)
   * Cross Pipe1 with Pipe2 for 10*9 = 90 tuples
   * Filter out tuples that are non-monotonic
   * For (t1,t2) we want t1<t2, otherwise reject.
   * This brings down 90 tuples to the desired 45 tuples = 10C2
   */
  def combinations[T](input: IndexedSeq[T], k: Int)(implicit flowDef: FlowDef, mode: Mode): Pipe = {

    // make k pipes with 1 column each
    // pipe 1 = 1 to n
    // pipe 2 = 2 to n
    // pipe 3 = 3 to n etc
    val n = input.size
    val allc = (1 to k).toList.map(x => Symbol("n" + x)) // all column names

    val pipes = allc.zipWithIndex.map(x => {
      val num = x._2 + 1
      val pipe = IterableSource((num to n), x._1).read
      (pipe, num)
    })

    val res = pipes.reduceLeft((a, b) => {
      val num = b._2
      val prevname = Symbol("n" + (num - 1))
      val myname = Symbol("n" + num)
      val mypipe = a._1
        .crossWithSmaller(b._1)
        .filter(prevname, myname){
          foo: (Int, Int) =>
            val (nn1, nn2) = foo
            nn1 < nn2
        }
      (mypipe, -1)
    })._1

    (1 to k).foldLeft(res)((a, b) => {
      val myname = Symbol("n" + b)
      val newname = Symbol("k" + b)
      a.map(myname -> newname){
        inpc: Int => input(inpc - 1)
      }.discard(myname)
    })

  }

  /**
   * Return a pipe with all nCk combinations, with k columns per row
   */
  def combinations(n: Int, k: Int)(implicit flowDef: FlowDef, mode: Mode) = combinations[Int]((1 to n).toArray, k)

  /**
   * Return a pipe with all nPk permutations, with k columns per row
   * For details, see combinations(...) above
   */

  def permutations[T](input: IndexedSeq[T], k: Int)(implicit flowDef: FlowDef, mode: Mode): Pipe = {

    val n = input.size
    val allc = (1 to k).toList.map(x => Symbol("n" + x)) // all column names

    val pipes = allc.map(x => IterableSource(1 to n, x).read)

    // on a given row, we cannot have duplicate columns in a permutation
    val res = pipes
      .reduceLeft((a, b) => { a.crossWithSmaller(b) })
      .filter(allc) {
        x: TupleEntry =>
          Boolean
          val values = (0 until allc.size).map(i => x.getInteger(i.asInstanceOf[java.lang.Integer]))
          values.size == values.distinct.size
      }

    // map numerals to actual data
    (1 to k).foldLeft(res)((a, b) => {
      val myname = Symbol("n" + b)
      val newname = Symbol("k" + b)
      a.map(myname -> newname){
        inpc: Int => input(inpc - 1)
      }.discard(myname)
    })

  }

  /**
   * Return a pipe with all nPk permutations, with k columns per row
   */
  def permutations(n: Int, k: Int)(implicit flowDef: FlowDef, mode: Mode) = permutations[Int]((1 to n).toArray, k)

  /**
   * Goal: Given weights (a,b,c, ...), we seek integers (x,y,z,...) to satisft
   * the constraint  |ax + by + cz + ... - result | < error
   *
   * Parameters: The weights (a,b,c,...) must be non-negative doubles.
   * Our search space is 0 to result/min(weights)
   * The returned pipe will contain integer tuples (x,y,z,...) that satisfy ax+by+cz +... = result
   *
   * Note: This is NOT Simplex
   * WE use a slughtly-improved brute-force algorithm that performs well on account of parallelization.
   * Algorithm:
   * Create as many pipes as the number of weights
   * Each pipe copntains integral multiples of the weight w ie. (0,1w,2w,3w,4w,....)
   * Iterate as below -
   * Cross two pipes
   * Create a temp column that stores intermediate results
   * Apply progressive filtering on the temp column
   * Discard the temp column
   * Once all pipes are crossed, test for temp column within error bounds of result
   * Discard duplicates at end of process
   *
   * Usecase: We'd like to generate all integer tuples for typical usecases like
   *
   * 0. How many ways can you invest $1000 in facebook, microsoft, hp ?
   * val cash = 1000.0
   * val error = 5.0 // max error $5, so its ok if we cannot invest the last $5 or less
   * val (FB, MSFT, HP) = (23.3,27.4,51.2) // share prices
   * val stocks = IndexedSeq( FB,MSFT,HP )
   * weightedSum( stocks, cash, error).write( Tsv("invest.txt"))
   *
   * 1. find all (x,y,z) such that 2x+3y+5z = 23, with max error 1
   * weightedSum( IndexedSeq(2.0,3.0,5.0), 23.0, 1.0)
   *
   * 2. find all (a,b,c,d) such that 2a+12b+12.5c+34.7d = 3490 with max error 3
   * weightedSum( IndexedSeq(2.0,12.0,2.5,34.7),3490.0,3.0)
   *
   * This is at the heart of portfolio mgmt( Markowitz optimization), subset-sum, operations-research LP problems.
   *
   */

  def weightedSum(weights: IndexedSeq[Double], result: Double, error: Double)(implicit flowDef: FlowDef, mode: Mode): Pipe = {
    val numWeights = weights.size
    val allColumns = (1 to numWeights).map(x => Symbol("k" + x))

    // create as many single-column pipes as the number of weights
    val pipes = allColumns.zip(weights).map(x => {
      val (name, wt) = x
      val points = Stream.iterate(0.0) { _ + wt }.takeWhile(_ <= result)
      IterableSource(points, name).read
    }).zip(allColumns)

    val first = pipes.head
    val accum = (first._1, List[Symbol](first._2))
    val rest = pipes.tail

    val res = rest.foldLeft(accum)((a, b) => {

      val (apipe, aname) = a
      val (bpipe, bname) = b
      val allc = (List(aname)).flatten ++ List[Symbol](bname)

      // Algorithm:
      // Cross two pipes
      // Create a temp column that stores intermediate results
      // Apply progressive filtering on the temp column
      // Discard the temp column
      // Once all pipes are crossed, test for temp column within error bounds of result
      // Discard duplicates at end of process

      (apipe.crossWithSmaller(bpipe)
        .map(allc -> 'temp){
          x: TupleEntry =>
            val values = (0 until allc.size).map(i => x.getDouble(i.asInstanceOf[java.lang.Integer]))
            values.sum
        }.filter('temp){
          x: Double => if (allc.size == numWeights) (math.abs(x - result) <= error) else (x <= result)
        }.discard('temp), allc)
    })._1.unique(allColumns)

    (1 to numWeights).zip(weights).foldLeft(res) ((a, b) => {
      val (num, wt) = b
      val myname = Symbol("k" + num)
      a.map(myname -> myname){ x: Int => (x / wt).toInt }
    })
  }

  /**
   * Does the exact same thing as weightedSum, but filters out tuples with a weight of 0
   * The returned pipe contain only positive non-zero weights.
   */
  def positiveWeightedSum(weights: IndexedSeq[Double], result: Double, error: Double)(implicit flowDef: FlowDef, mode: Mode): Pipe = {
    val allColumns = (1 to weights.size).map(x => Symbol("k" + x))
    weightedSum(weights, result, error)
      .filter(allColumns) { x: TupleEntry =>
        (0 until allColumns.size).forall { i => x.getDouble(java.lang.Integer.valueOf(i)) != 0.0 }
      }
  }

}
