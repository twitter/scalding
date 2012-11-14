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

  // helper class
  case class PN(pipe:RichPipe, number:Int)

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
  def combinations[T](input:IndexedSeq[T], k:Int)(implicit flowDef:FlowDef):RichPipe = {



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
      (1 to k).foldLeft(res)((a,b)=>{
        val myname = Symbol( "n" + b)
        val newname = Symbol("k" + b)
        a.map(myname->newname){
          inpc:Int => input(inpc-1)
        }.discard(myname)
      })

  }

  /**
  Return a pipe with all nCk combinations, with k columns per row
  */
  def combinations(n:Int, k:Int)(implicit flowDef:FlowDef) = combinations[Int]((1 to n).toArray, k)

  /**
  Return a pipe with all nPk permutations, with k columns per row
  For details, see combinations(...) above
  */
  def permutations[T](input:IndexedSeq[T], k:Int)(implicit flowDef:FlowDef):RichPipe = {

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
      (1 to k).foldLeft(res)((a,b)=>{
        val myname = Symbol( "n" + b)
        val newname = Symbol("k" + b)
        a.map(myname->newname){
          inpc:Int => input(inpc-1)
        }.discard(myname)
      })

  }

  /**
  Return a pipe with all nPk permutations, with k columns per row
  */
  def permutations(n:Int, k:Int)(implicit flowDef:FlowDef) = combinations[Int]((1 to n).toArray, k)

  /**
  Want k-combinations of stuff, with repititions, that satisfy some predicate

  eg. want all non-negative (a,b,c) such that a+b+c = 100
  This can be phrased as
  generateTuples( (0 to 100).toList, 3, ((a,b)=>a+b), (x=>x>100), (x=> x==100))

  eg2. want all non-negative (a,b,c) such that (abc)^2 < 1000
  generateTuples( (1 to 33).toList, 3, (a,b)=>a*a*b*b), (x=> x >= 1000) , (x=> x<1000))

  eg3. want all non-negative integral vectors in 3 space of length smaller than 15
  ie. (a,b,c) such that a^2 + b^2 + c^2 < 225
  generateTuples( (0 to 15).toList, 3, ((a,b)=>math.sqrt(a*a+b*b)), (x=> x> 15), (x=> x<15))

  NOTE: f(a,b) must have same semantics as f(f(a,b), c) for all of this to work.

  */
  def generateTuples[T]( stuff:IndexedSeq[T], k:Int,op:((T,T)=>T), resultNotOK:(T => Boolean), resultOK:(T => Boolean))(implicit flowDef:FlowDef):RichPipe = {

    // make k pipes
    val n = stuff.size
    val pipes = (1 to k).foldLeft(List[PN]())( (a,x) => {
    val myname = Symbol("k"+x)
    val pipe = IterableSource(List(""+n), 'n).read.flatMap('n->myname) { x:String => stuff }.project(myname)
      PN(pipe, x)::a
    })

    // filter progressively
    val rev = pipes.reverse
    val myhead = rev.head
    val mytail = rev.tail
    val dummy = Symbol("g1")
    val head2 = myhead.pipe.map('k1->dummy){
      x:String => null.asInstanceOf[T]
    }
    val res = mytail.foldLeft( PN(head2,-1) )( (a,b) => {
        val num = b.number // what pipe are we currently processing
        val prevname = Symbol("k" + (num - 1))
        val myname = Symbol( "k" + num)
        val newc = Symbol( "g" + num)
        val oldc = Symbol("g" + (num-1))
        val mypipe = a.pipe
                    .crossWithSmaller(b.pipe)
                    .map( (prevname, myname,oldc) -> newc ){
                      foo:(T,T,T) =>
                      val (nn1, nn2,nn3) = foo
                      op( if( nn3 == null ) nn1 else nn3, nn2 )
                    }
                    .filter( newc ){
                      foo:T =>
                      if( newc.eq( Symbol( "g"+k) )) {
                        resultOK(foo)
                      }
                      else {
                        !resultNotOK( foo )
                      }
                    }
          PN( mypipe, -1)
    }).pipe

    (1 to k).foldLeft( res )((a,b)=>{
        a.discard( Symbol("g"+ b))
    })

  }

}
