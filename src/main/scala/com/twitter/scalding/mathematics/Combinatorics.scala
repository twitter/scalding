package com.twitter.scalding.mathematics
import com.twitter.scalding._
import com.twitter.scalding.Dsl._
import cascading.flow.FlowDef
import cascading.tuple.{Fields, TupleEntry}
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
  type PipeInt = (RichPipe, Int)

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
    val allc = (1 to k).toList.map( x=> Symbol("n"+x)) // all column names

    val pipes = allc.zipWithIndex.map( x=> {
        val pipe = IterableSource(List(""+n), 'n).read.flatMap('n->x._1) {
          s:String => (x._2+1 to n).toList
        }.project(x._1)

        val pn:PipeInt = (pipe, x._2 + 1)
        pn
    })

    val res = pipes.reduceLeft( (a,b) => {
      val num = b._2
      val prevname = Symbol("n" + (num - 1))
      val myname = Symbol( "n" + num)
      val mypipe = a._1
                  .crossWithSmaller(b._1)
                  .filter( prevname, myname ){
                    foo:(Int, Int) =>
                    val( nn1, nn2) = foo
                    nn1 < nn2
                  }
      (mypipe, -1)
    })._1

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

    val n = input.size
    val allc = (1 to k).toList.map( x=> Symbol("n"+x)) // all column names

    val pipes = allc.map( x=> {
        IterableSource(List(""+n), 'n).read.flatMap('n->x) {
          s:String => (1 to n).toList
        }.project(x)
    })

    // on a given row, we cannot have duplicate columns in a permutation
    val res = pipes
              .reduceLeft( (a,b) => { a.crossWithSmaller(b) })
              .filter( allc ) {
                 x: TupleEntry => Boolean
                val values = (0 to k-1).toList.map( f=> x.getObject(f).asInstanceOf[Int] )
                values.size == values.distinct.size
              }

     // map numerals to actual data
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
  Note: f(a,b,c) must have same semantics as f(f(a,b), c) for all of this to work.

  stuff: Elements the k-combinations are composed of.

  k : The arity of the k-combination

  op: How to compose ANY TWO elements of the k-combination.
  ie. the first two, the cumulative result of the first two & the third, etc.

  progressiveFilter: We first generate 2-tuples, then 3-tuples, then 4-tuples etc.
  At each stage, we filter.
  The progressiveFilter must reject those tuples that satisfy this filter
  ie. if (x=>x>15) is the progressiveFilter, then all x's who exceed 15 will be excluded.

  goal: The k-tuple must satisfy this goal
  ie. if we have a 4-tuple & the goal is (x=>x==17), with an op of (a,b=>a+b)
  then we want a+b+c+d == 17

  Typical Usecases:
  eg. want all non-negative (a,b,c) such that a+b+c = 100
  f(a,b,c) = a+b+c = (f(a,b),c), so we are good.
  This can be phrased as
  generateTuples( (0 to 100).toIndexedSeq, 3, ((a,b)=>a+b), (x=>x>100), (x=> x==100))

  eg2. want all non-negative (a,b,c) such that (abc)^2 < 1000
  generateTuples( (1 to 33).toIndexedSeq, 3, (a,b)=>a*a*b*b), (x=> x >= 1000) , (x=> x<1000))

  eg3. want all vectors in 3 space of length less than 20
  ie. (a,b,c) such that a^2 + b^2 + c^2 < 400
  generateTuples[Double]( (0.0 to 20.0 by 0.1).toIndexedSeq, 3, ((a,b)=>math.sqrt(a*a+b*b)), (x=>x>20.0), (x=> x<20.0))
  Notice how we CANNOT use (a,b)=>a*a+b*b as the op, because f(a,b,c) != f(f(a,b),c) in that case !!!

  */
  def generateTuples[T]( stuff:IndexedSeq[T], k:Int,op:((T,T)=>T), progressiveFilter:(T => Boolean), goal:(T => Boolean))(implicit flowDef:FlowDef) :RichPipe = {

    // make k pipes
    val n = stuff.size
    val pipes = (1 to k).foldLeft(List[PipeInt]())( (a,x) => {
    val myname = Symbol("k"+x)
    val pipe = IterableSource(List(""+n), 'n).read.flatMap('n->myname) { x:String => stuff }.project(myname)
      val pn:PipeInt = (pipe,x)
      pn::a
    })

    // filter progressively
    val rev = pipes.reverse
    val myhead = rev.head
    val mytail = rev.tail
    val dummy = Symbol("g1")
    val head2 = myhead._1.map('k1->dummy){ x:String => null.asInstanceOf[T] }
    val accum:PipeInt = (head2,-1)
    val res = mytail.foldLeft( accum )( (a,b) => {
        val num = b._2 // what pipe are we currently processing
        val prevname = Symbol("k" + (num - 1))
        val myname = Symbol( "k" + num)
        val newc = Symbol( "g" + num)
        val oldc = Symbol("g" + (num-1))
        val mypipe = a._1
                    .crossWithSmaller(b._1)
                    .map( (prevname, myname,oldc) -> newc ){
                      foo:(T,T,T) =>
                      val (nn1, nn2,nn3) = foo
                      op( if( nn3 == null ) nn1 else nn3, nn2 )
                    }
                    .filter( newc ){
                      foo:T =>
                      if( newc.eq( Symbol( "g"+k) )) {
                        goal(foo)
                      }
                      else {
                        !progressiveFilter( foo )
                      }
                    }

          val pn:PipeInt = (mypipe, -1)
          pn
    })._1

    (1 to k).foldLeft( res )((a,b)=>{
        a.discard( Symbol("g"+ b))
    })
  }

  /**
  We'd like to generate all integer tuples for typical usecases like

  0. How many ways can you invest $1000 in facebook, microsoft, hp ?
  val cash = 1000
  val error = 5 // max error $5, so its ok if we cannot invest the last $5 or less
  val (FB, MSFT, HP) = (23,27,51) // share prices
  val stocks = IndexedSeq( FB,MSFT,HP )
  val (minShares,maxShares) = (1,1000/23) // about 50 shares max, since min stock price = 23, 23*50 > 1000
  weightedSum( (minShares,maxShares), stocks, cash, error).write( Tsv("invest.txt"))

  1. find all (x,y,z) in [1,100] such that 2x+3y+5z = 23, with max error 1
  weightedSum( (1,100), IndexedSeq(2,3,5), 23, 1)

  2. find all (a,b,c,d) in [1,100] such that 2a+12b+12.5c+34.7d = 3490 with max error 3
  weightedSum( (1,100), IndexedSeq(2,12,2.5,34.7),3490,3)

  This is at the heart of portfolio mgmt: Markowitz optimization & the like

  While this problem is not directly mappable to generateTuples,
  a combination of generateTuples, filter(...) and map(...) can produce a solution.
  */

  def weightedSum( minmax:(Int, Int), weights:IndexedSeq[Double], result:Double, error:Double)(implicit flowDef:FlowDef) :RichPipe = {
    val k = weights.size
    val allc = (1 to k).toList.map( x=> Symbol("k"+x)) // all column names

    // recast problem as a u+v+w = result
    val uvw = generateTuples[Int]( minmax._1 to (minmax._2*weights.max).toInt, k, ((a,b)=>a+b), (x=> (x > result)), (x=>x==result))

    // get all integers x so a*x = u for given weight a
    // get all integers y so b*y = v for given weight b
    // get all integers z so c*z = w for given weight c
    (1 to k).toList.zip(weights).foldLeft(uvw)( (res,b)=> {
      val (idx,wt) = b
      val name = Symbol("k"+idx)
      res.map( name -> name){
        c:Int => (c/wt).toInt
      }
    }).filter(allc) {
      x: TupleEntry => Boolean
      val values = (0 to k-1).toList.map( f=> x.getObject(f).asInstanceOf[Int] )
      val sum = values.zip(weights).map(ab=>ab._1*ab._2).sum
      ( values.map( x=> (x >=minmax._1 && x<= minmax._2 )).reduceLeft((a,b)=>a&&b) && math.abs(sum -result) < error)
    }.unique(allc)
  }


}
