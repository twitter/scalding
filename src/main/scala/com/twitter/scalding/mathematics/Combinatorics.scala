package com.twitter.scalding.mathematics
import com.twitter.scalding._
import com.twitter.scalding.Dsl._
import cascading.flow.FlowDef
/**
Serve as a repo for self-contained combinatorial functions with no dependencies
such as
combinations, aka n choose k, nCk
permutations , aka nPk
catalan numbers
numbers that add up to a finite sum
...
@author : kraman@twitter.com
*/

object Combinatorics {
/**
  Given an int k, and an input of size n,
  return a pipe with nCk combinations, with k columns per row


  Computes nCk = n choose k, for large values of nCk

  Use-case: Say you have 100 hashtags sitting in an array
  You want a table with 5 hashtags per row, all possible combinations
  But 100C5 is huge!!! Scala cannot handle it. But Scalding can!
  If the hashtags are sitting in a string array, then
  combinations[String]( hashtags, 5)
  will do the needful.

  Algorithm: Use k pipes, cross pipes two at a time, filter out non-monotonic entries

  eg. 10C2 = 10 choose 2
  Use 2 pipes.
  Pipe1 = (1,2,3,...10)
  Pipe2 = (2,3,4....10)
  Cross Pipe1 with Pipe2 for 10*9 = 90 tuples
  Filter out tuples that are non-monotonic
  For (t1,t2) we want t1<t2, otherwise reject.
  This brings down 90 tuples to the desired 45 tuples = 10C2
  */
  def combinations[T](input:Array[T], k:Int)(implicit flowDef:FlowDef):RichPipe = {

      case class PN(pipe:RichPipe, number:Int)

      // make k pipes with 1 column each
      // pipe 1 = 1 to n
      // pipe 2 = 2 to n
      // pipe 3 = 3 to n etc
      val n = input.size
      val pipes = (1 to k).foldLeft(List[PN]())( (a,x) => {
        val myname = Symbol("n"+x)
        val pipe = IterableSource(List(""+n), 'n).read.flatMap('n->myname) {
            n:String =>
            val nn = n.toInt
            (x to nn).toList
        }.project(myname)

        PN(pipe, x)::a
      })

      // cross k pipes with 1 column each to get 1 pipe with k columns
      // filter out spurious entries while crossing
      val res = pipes.reverse.reduceLeft( (a,b) => {
        val num = b.number
        val prevname = Symbol("n" + (num - 1))
        val myname = Symbol( "n" + num)
        val mypipe = a.pipe
                    .crossWithSmaller(b.pipe)
                    .filter( prevname, myname ){
                      foo:(Int, Int) =>
                      val( nn1, nn2) = foo
                      nn1 < nn2
                    }
        PN( mypipe, -1)

      }).pipe

      // map the numeric tuples to data tuples
      val res2 = (1 to k).foldLeft(res)((a,b)=>{
        val myname = Symbol( "n" + b)
        val newname = Symbol("k" + b)
        a.map(myname->newname){
          inpc:Int => input(inpc-1)
        }.discard(myname)
      })

      res2
  }

  /**
  Return a pipe with all nCk combinations, with k columns per row
  */
  def combinations(n:Int, k:Int)(implicit flowDef:FlowDef) = combinations[Int]((1 to n).toArray, k)

  /**
  Return a pipe with all nPk permutations, with k columns per row
  For details, see combinations(...) above
  */
  def permutations[T](input:Array[T], k:Int)(implicit flowDef:FlowDef):RichPipe = {

      case class PN(pipe:RichPipe, number:Int)

      // make k pipes with 1 column each
      // pipe 1 = 1 to n
      // pipe 2 = 2 to n
      // pipe 3 = 3 to n etc
      val n = input.size
      val pipes = (1 to k).foldLeft(List[PN]())( (a,x) => {
        val myname = Symbol("n"+x)
        val pipe = IterableSource(List(""+n), 'n).read.flatMap('n->myname) {
            n:String =>
            val nn = n.toInt
            (x to nn).toList
        }.project(myname)

        PN(pipe, x)::a
      })

      // cross k pipes with 1 column each to get 1 pipe with k columns
      // filter out spurious entries while crossing
      val res = pipes.reverse.reduceLeft( (a,b) => {
        val num = b.number
        val prevname = Symbol("n" + (num - 1))
        val myname = Symbol( "n" + num)
        val mypipe = a.pipe
                    .crossWithSmaller(b.pipe)
                    .filter( prevname, myname ){
                      foo:(Int, Int) =>
                      val( nn1, nn2) = foo
                      nn1 != nn2
                    }
        PN( mypipe, -1)

      }).pipe

      // map the numeric tuples to data tuples
      val res2 = (1 to k).foldLeft(res)((a,b)=>{
        val myname = Symbol( "n" + b)
        val newname = Symbol("k" + b)
        a.map(myname->newname){
          inpc:Int => input(inpc-1)
        }.discard(myname)
      })

      res2
  }

  /**
  Return a pipe with all nPk permutations, with k columns per row
  */
  def permutations(n:Int, k:Int)(implicit flowDef:FlowDef) = combinations[Int]((1 to n).toArray, k)

}
