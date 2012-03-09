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
package com.twitter.scalding

import cascading.pipe.Pipe
import cascading.pipe.Every
import cascading.pipe.GroupBy
import cascading.pipe.assembly._
import cascading.operation._
import cascading.operation.aggregator._
import cascading.operation.filter._
import cascading.tuple.Fields

import scala.annotation.tailrec
import scala.math.Ordering

// This controls the sequence of reductions that happen inside a
// particular grouping operation.  Not all elements can be combined,
// for instance, a scanLeft/foldLeft generally requires a sorting
// but such sorts are (at least for now) incompatible with doing a combine
// which includes some map-side reductions.
class GroupBuilder(val groupFields : Fields) extends FieldConversions
  with TupleConversions with java.io.Serializable {

  /**
  * Holds the "reducers/combiners", the things that we can do paritially map-side.
  */
  private var reds : Option[List[AggregateBy]] = Some(Nil)

  /**
  * This is the description of this Grouping in terms of a sequence of Every operations
  */
  private var evs : List[Pipe => Every] = Nil
  private var isReversed : Boolean = false
  private var sortBy : Option[Fields] = None
  /*
  * maxMF is the maximum index of a "middle field" allocated for mapReduceMap operations
  */
  private var maxMF : Int = 0

  private def getNextMiddlefield : String = {
    val out = "__middlefield__" + maxMF.toString
    maxMF += 1
    return out
  }

  //Put any pure reduce functions into the below object
  import CommonReduceFunctions._

  def aggregate(args : Fields)(a : Aggregator[_]) : GroupBuilder = {
    every(pipe => new Every(pipe, args, a))
  }

  private def tryAggregateBy(ab : AggregateBy, ev : Pipe => Every) : Boolean = {
    // Concat if there if not none
    reds = reds.map(rl => ab::rl)
    evs = ev :: evs
    return !reds.isEmpty
  }

  /**
  * Holds the number of reducers to use in the reduce stage of the groupBy/aggregateBy.
  * By default uses whatever value is set in the jobConf.
  */
  private var numReducers : Option[Int] = None
  private val REDUCER_KEY = "mapred.reduce.tasks"
  /**
   * Override the number of reducers used in the groupBy.
   */
  def reducers(r : Int) = {
    if(r > 0) {
      numReducers = Some(r)
    }
    this
  }

  private def overrideReducers(p : Pipe) : Pipe = {
    numReducers.map{ r =>
      if(r <= 0)
        throw new IllegalArgumentException("Number of reducers must be non-negative")
      p.getProcessConfigDef()
        .setProperty(REDUCER_KEY, r.toString)
    }
    p
  }

  // When combining averages, if the counts sizes are too close we should use a different
  // algorithm.  This constant defines how close the ratio of the smaller to the total count
  // can be:
  private val STABILITY_CONSTANT = 0.1
  /**
   * uses a more stable online algorithm which should
   * be suitable for large numbers of records
   * similar to:
   * http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
   */
  def average(f : (Fields, Fields)) : GroupBuilder = {
    mapReduceMap(f){(x:Double) =>
      (1L, x)
    } {(cntAve1, cntAve2) =>
      val (big, small) = if (cntAve1._1 >= cntAve2._1) (cntAve1, cntAve2) else (cntAve2, cntAve1)
      val n = big._1
      val k = small._1
      val an = big._2
      val ak = small._2
      val newCnt = n+k
      val scaling = k.toDouble/newCnt
      // a_n + (a_k - a_n)*(k/(n+k)) is only stable if n is not approximately k
      val newAve = if (scaling < STABILITY_CONSTANT) (an + (ak - an)*scaling) else (n*an + k*ak)/newCnt
      (newCnt, newAve)
    } { res => res._2 }
  }
  def average(f : Symbol) : GroupBuilder = average(f->f)

  // WARNING! This may significantly reduce performance of your job.
  // It kills the ability to do map-side aggregation.
  def buffer(args : Fields)(b : Buffer[_]) : GroupBuilder = {
    every(pipe => new Every(pipe, args, b))
  }

  // By default adds a column with name "count" counting the number in
  // this group. deprecated, use size.
  @deprecated("Use size instead to match the scala.collections.Iterable API")
  def count(f : Symbol = 'count) : GroupBuilder = size(f)

  // This is count with a predicate: only counts the tuples for which fn(tuple) is true
  def count[T:TupleConverter](fieldDef : (Fields, Fields))(fn : T => Boolean) : GroupBuilder = {
    mapReduceMap[T,Long,Long](fieldDef)(arg => if(fn(arg)) 1L else 0L)((s1 : Long, s2 : Long) => s1+s2)(s => s)
  }

  /**
   * Convert a subset of fields into a list of Tuples. Need to provide the types of the tuple fields.
   * Note that the order of the tuples is not preserved.
   */
  def toList[T](fieldDef : (Fields, Fields))(implicit conv : TupleConverter[T]) : GroupBuilder = {
    val (fromFields, toFields) = fieldDef
    conv.assertArityMatches(fromFields)
    val out_arity = toFields.size
    assert(out_arity == 1, "toList: can only add a single element to the GroupBuilder")
    mapReduceMap[T, List[T], List[T]](fieldDef) { //Map
      x => Option(x).map{ List(_) }.getOrElse(List())
    } { //Reduce
      (t1, t2) => t2 ++ t1
    } { //Map
      t => t
    }
  }

  /**
   * Compute the count, ave and stdard deviation in one pass
   * example: g.cntAveStdev('x -> ('cntx, 'avex, 'stdevx))
   * uses: http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
   */
  def sizeAveStdev(fieldDef : (Fields,Fields)) = {
    val (fromFields, toFields) = fieldDef
    val in_arity = fromFields.size
    val out_arity = toFields.size
    assert(in_arity == 1, "cntAveVar: Can only take the moment of a single arg")
    assert(out_arity == 3, "cntAveVar: Need names for cnt, ave and var moments")
    // unbiased estimator: sqrt((1/(N-1))(sum_i(x_i - ave)^2))
    // sum_i (x_i - ave)^2 = sum_i x_i^2 - 2x_i ave +ave^2
    //                     = (sum_i x_i^2) - 2N ave^2 + N ave^2
    //                     = (sum_i x_i^2) - N ave^2
    mapReduceMap[Double, (Long,Double,Double), (Long,Double,Double)](fieldDef) { //Map
      (x : Double) => (1L,x,0.0)
    } {(cntAve1, cntAve2) =>
      val (big, small) = if (cntAve1._1 >= cntAve2._1) (cntAve1, cntAve2) else (cntAve2, cntAve1)
      val n = big._1
      val k = small._1
      val an = big._2
      val ak = small._2
      val delta = (ak - an)
      val mn = big._3
      val mk = small._3
      val newCnt = n+k
      val scaling = k.toDouble/newCnt
      // a_n + (a_k - a_n)*(k/(n+k)) is only stable if n is not approximately k
      val newAve = if (scaling < STABILITY_CONSTANT) (an + delta*scaling) else (n*an + k*ak)/newCnt
      val newStdMom = mn + mk + delta*delta*(n*scaling)
      (newCnt, newAve, newStdMom)
    } { //Map
      moms =>
        val cnt = moms._1
        (cnt, moms._2, scala.math.sqrt(moms._3/(cnt - 1)))
    }
  }

  //Remove the first cnt elements
  def drop(cnt : Int) : GroupBuilder = {
    val b = new DropBuffer(cnt)
    every(pipe => new Every(pipe, Fields.VALUES, b, Fields.REPLACE))
  }
  //Drop while the predicate is true, starting at the first false, output all
  def dropWhile[T](f : Fields)(fn : T => Boolean)(implicit conv : TupleConverter[T]) : GroupBuilder = {
    conv.assertArityMatches(f)
    every(pipe => new Every(pipe, f, new DropWhileBuffer[T](fn, conv), Fields.REPLACE))
  }

  //Prefer aggregateBy operations!
  def every(ev : Pipe => Every) : GroupBuilder = {
    reds = None
    evs = ev :: evs
    this
  }

  /*
   *  prefer reduce or mapReduceMap. foldLeft will force all work to be
   *  done on the reducers.  If your function is not associative and
   *  commutative, foldLeft may be required.
   */
  def foldLeft[X,T](fieldDef : (Fields,Fields))(init : X)(fn : (X,T) => X)
                 (implicit setter : TupleSetter[X], conv : TupleConverter[T]) : GroupBuilder = {
      val (inFields, outFields) = fieldDef
      conv.assertArityMatches(inFields)
      setter.assertArityMatches(outFields)
      val ag = new FoldAggregator[T,X](fn, init, outFields, conv, setter)
      every(pipe => new Every(pipe, inFields, ag))
  }

  /*
   * check if a predicate is satisfied for all in the values for this key
   */
  def forall[T:TupleConverter](fieldDef : (Fields,Fields))(fn : (T) => Boolean) : GroupBuilder = {
    mapReduceMap(fieldDef)(fn)({(x : Boolean, y : Boolean) => x && y})({ x => x })
  }

  // Return the first, useful probably only for sorted case.
  def head(f : Fields) = aggregate(f)(new First())
  def last(f : Fields) = aggregate(f)(new Last())

  private def extremum(max : Boolean, fieldDef : (Fields,Fields)) : GroupBuilder = {
    val (fromFields, toFields) = fieldDef
    val in_arity = fromFields.size
    val out_arity = toFields.size
    assert(in_arity == out_arity, "Number of field names must match for rename")
    //Now do the work:
    val ag = new ExtremumAggregator(max, toFields)
    val ev = (pipe => new Every(pipe, fromFields, ag)) : Pipe => Every
    tryAggregateBy(new ExtremumBy(max, fromFields, toFields), ev)
    this
  }

  /**
  * Type T is the type of the input field (input to map, T => X)
  * Type X is the intermediate type, which your reduce function operates on
  * (reduce is (X,X) => X)
  * Type U is the final result type, (final map is: X => U)
  */
  def mapReduceMap[T,X,U](fieldDef : (Fields, Fields))(mapfn : T => X )(redfn : (X, X) => X)
      (mapfn2 : X => U)(implicit startConv : TupleConverter[T],
                        middleSetter : TupleSetter[X],
                        middleConv : TupleConverter[X],
                        endSetter : TupleSetter[U]) : GroupBuilder = {
    val (fromFields, toFields) = fieldDef
    //Check for arity safety:
    startConv.assertArityMatches(fromFields)
    endSetter.assertArityMatches(toFields)

    val ag = new MRMAggregator[T,X,U](mapfn, redfn, mapfn2, toFields, startConv, endSetter)
    val ev = (pipe => new Every(pipe, fromFields, ag)) : Pipe => Every

    // Create the required number of middlefields based on the arity of middleSetter
    val middleFields = strFields( Range(0, middleSetter.arity).map{i => getNextMiddlefield} )
    val mrmBy = new MRMBy[T,X,U](fromFields, middleFields, toFields,
      mapfn, redfn, mapfn2, startConv, middleSetter, middleConv, endSetter)
    tryAggregateBy(mrmBy, ev)
    this
  }

  def max(fieldDef : (Fields, Fields)) = extremum(true, fieldDef)
  def max(fieldDef : Symbol*) = {
    val f : Fields = fieldDef
    extremum(true, (f,f))
  }
  def min(fieldDef : (Fields, Fields)) = extremum(false, fieldDef)
  def min(fieldDef : Symbol*) = {
    val f : Fields = fieldDef
    extremum(false, (f,f))
  }
  /*
   * similar to the scala.collection.Iterable.mkString
   * takes the source and destination fieldname, which should be a single
   * field.
   * the result will be start, each item.toString separated by sep, followed
   * by end
   * for convenience there several common variants below
   */
  def mkString(fieldDef : (Fields,Fields), start : String, sep : String, end : String) : GroupBuilder = {
    val (inFields, outFields) = fieldDef
    val in_arity = inFields.size
    val out_arity = outFields.size
    assert(in_arity == 1, "mkString works on single column, concat in a map before, if you need.")
    assert(out_arity == 1, "output field count must also be 1")
    /*
     * Logically a mapReduceMap works here, but it does O(N) string
     * concats, if each are order m in length which costs (\sum_{i=1}^N  i\times m)
     * which is m N^2/2 cost. We can do O(N) if we allocate once long enough
     * for all N items, and then copy, which is what
     * Iterable.mkString does
     */
    val mkag = new MkStringAggregator(start, sep, end, outFields)
    val ev = (pipe => new Every(pipe, inFields, mkag)) : Pipe => Every
    tryAggregateBy(new MkStringBy(start, sep, end, inFields, outFields), ev)
    this
  }
  def mkString(fieldDef : (Fields,Fields), sep : String) : GroupBuilder = mkString(fieldDef,"",sep,"")
  def mkString(fieldDef : (Fields,Fields)) : GroupBuilder = mkString(fieldDef,"","","")
  /**
  * these will only be called if a tuple is not passed, meaning just one
  * column
  */
  def mkString(fieldDef : Symbol, start : String, sep : String, end : String) : GroupBuilder = {
    val f : Fields = fieldDef
    mkString((f,f),start,sep,end)
  }
  def mkString(fieldDef : Symbol, sep : String) : GroupBuilder = mkString(fieldDef,"",sep,"")
  def mkString(fieldDef : Symbol) : GroupBuilder = mkString(fieldDef,"","","")

  def reduce[T](fieldDef : (Fields, Fields))(fn : (T,T)=>T)
               (implicit setter : TupleSetter[T], conv : TupleConverter[T]) : GroupBuilder = {
    val (inFields, outFields) = fieldDef
    val in_arity = inFields.size
    val out_arity = outFields.size
    assert(in_arity == out_arity, "Reduce output arity must match input arity")
    //Check arity of setter/conv
    conv.assertArityMatches(inFields)
    setter.assertArityMatches(outFields)
    //Now do the work:
    val ag = new MRMAggregator[T,T,T](args => args, fn, x => x, outFields, conv, setter)
    val ev = (pipe => new Every(pipe, inFields, ag)) : Pipe => Every
    tryAggregateBy(new CombineBy[T](inFields, outFields, fn, conv, setter), ev)
    this
  }
  //Same as reduce(f->f)
  def reduce[T](fieldDef : Symbol*)(fn : (T,T)=>T)(implicit setter : TupleSetter[T],
                                 conv : TupleConverter[T]) : GroupBuilder = {
    reduce(fieldDef -> fieldDef)(fn)(setter,conv)
  }

  def reverse : GroupBuilder = {
    assert(reds.isEmpty, "Cannot sort when reducing")
    assert(!isReversed, "Reverse called a second time! Only one allowed")
    isReversed = true
    this
  }

  //This invalidates map-side aggregation, forces all data to be transferred
  //to reducers.  Use only if you REALLY have to.
  def scanLeft[X,T](fieldDef : (Fields,Fields))(init : X)(fn : (X,T) => X)
                 (implicit setter : TupleSetter[X], conv : TupleConverter[T]) : GroupBuilder = {
    val (inFields, outFields) = fieldDef
    //Check arity
    conv.assertArityMatches(inFields)
    setter.assertArityMatches(outFields)

    val b = new ScanBuffer[T,X](fn, init, outFields, conv, setter)
    every(pipe => new Every(pipe, inFields, b))
  }

  def schedule(name : String, allpipes : Pipe*) : Pipe = {
    val mpipes : Array[Pipe] = allpipes.toArray
    reds match {
      case None => {
        //We cannot aggregate, so group:
        val startPipe : Pipe = sortBy match {
          case None => new GroupBy(name, mpipes, groupFields)
          case Some(sf) => new GroupBy(name, mpipes, groupFields, sf, isReversed)
        }
        overrideReducers(startPipe)

        // Time to schedule the addEverys:
        evs.foldRight(startPipe)( (op : Pipe => Every, p) => op(p) )
      }
      //This is the case where the group function is identity: { g => g }
      case Some(Nil) => {
        val gb = new GroupBy(name, mpipes, groupFields)
        overrideReducers(gb)
        gb
      }
      //There is some non-empty AggregateBy to do:
      case Some(redlist) => {
        val THRESHOLD = 100000 //tune this, default is 10k
        val ag = new AggregateBy(name, mpipes, groupFields,
          THRESHOLD, redlist.reverse.toArray : _*)
        overrideReducers(ag.getGroupBy())
        ag
      }
    }
  }

  //This invalidates aggregateBy!
  def sortBy(f : Fields) : GroupBuilder = {
    reds = None
    sortBy = sortBy match {
      case None => Some(f)
      case Some(sf) => {
        sf.append(f)
        Some(sf)
      }
    }
    this
  }

  //How many values are there for this key
  def size : GroupBuilder = size('size)
  def size(inf : Symbol) : GroupBuilder = {
      val thisF : Fields = inf
      assert(thisF.size == 1, "size only gives a single column output")
      //Count doesn't need inputs, but if you use Fields.ALL it will
      //fail if it comes after any other Every.
      val ev = (pipe => new Every(pipe, Fields.VALUES, new Count(thisF))) : Pipe => Every
      tryAggregateBy(new CountBy(thisF), ev)
      this
  }

  def sum(f : (Fields, Fields)) : GroupBuilder = {
    val (input, output) = f
    val in_arity = input.size
    val out_arity = input.size
    assert(in_arity == 1, "size can only sum a single column")
    assert(out_arity == 1, "output field size must also be 1")
    val ag = new Sum(output)
    val ev = (pipe => new Every(pipe, input, ag)) : Pipe => Every
    tryAggregateBy(new SumBy(input, output, java.lang.Double.TYPE), ev)
    this
  }
  def sum(f : Symbol) : GroupBuilder = {
    //Implicitly convert to a pair of fields:
    val field : Fields = f
    sum(field -> field)
  }
  //Only keep the first cnt elements
  def take(cnt : Int) : GroupBuilder = {
    val b = new TakeBuffer(cnt)
    every(pipe => new Every(pipe, Fields.VALUES, b, Fields.REPLACE))
  }
  //Take while the predicate is true, starting at the first false, output all
  def takeWhile[T](f : Fields)(fn : (T) => Boolean)(implicit conv : TupleConverter[T]) : GroupBuilder = {
    conv.assertArityMatches(f)
    every(pipe => new Every(pipe, f, new TakeWhileBuffer[T](fn, conv), Fields.REPLACE))
  }

  // This is convenience method to allow plugging in blocks of group operations
  // similar to RichPipe.then
  def then(fn : (GroupBuilder) => GroupBuilder) = fn(this)

  // Equivalent to sorting by a comparison function
  // then take-ing k items.  This is MUCH more efficient than doing a total sort followed by a take,
  // since these bounded sorts are done on the mapper, so only a sort of size k is needed.
  // example:
  // sortWithTake( ('clicks, 'tweet) -> 'topClicks, 5) { fn : (t0 :(Long,Long), t1:(Long,Long) => t0._1 < t1._1 }
  // topClicks will be a List[(Long,Long)]
  def sortWithTake[T:TupleConverter](f : (Fields, Fields), k : Int)(lt : (T,T) => Boolean) : GroupBuilder = {
    assert(f._2.size == 1, "output field size must be 1")
    mapReduceMap(f) /* map1 */ { (tup : T) => List(tup) }
    /* reduce */ { (l1 : List[T], l2 : List[T]) =>
      mergeSorted(l1, l2, lt, k)
    } /* map2 */ {
      (lout : List[T]) => lout
    }
  }

  // Reverse of above when the implicit ordering makes sense.
  def sortedReverseTake[T](f : (Fields, Fields), k : Int)
    (implicit conv : TupleConverter[T], ord : Ordering[T]) : GroupBuilder = {
    sortWithTake(f,k) { (t0:T,t1:T) => ord.gt(t0,t1) }
  }

  // Same as above but useful when the implicit ordering makes sense.
  def sortedTake[T](f : (Fields, Fields), k : Int)
    (implicit conv : TupleConverter[T], ord : Ordering[T]) : GroupBuilder = {
    sortWithTake(f,k) { (t0:T,t1:T) => ord.lt(t0,t1) }
  }
}

/*
 * These need to be serializable, but GroupBuilder has state and is a larger object,
 * so it is not ideal to make it serializable
 */
object CommonReduceFunctions extends java.io.Serializable {
  /*
   * merge two sorted lists.
   */
  final def mergeSorted[T](v1 : List[T], v2 : List[T], lt : (T,T) => Boolean, k : Int = -1) : List[T] = {
    @tailrec
    //This is the internal loop that does one comparison:
    def mergeSortR(acc : List[T], list1 : List[T], list2 : List[T], k : Int) : List[T] = {
      (list1, list2, k) match {
        case (_,_,0) => acc
        case (x1 :: t1, x2 :: t2, _) => {
          if( lt(x1,x2) ) {
            mergeSortR(x1 :: acc, t1, list2, k-1)
          }
          else {
            mergeSortR(x2 :: acc, list1, t2, k-1)
          }
        }
        case (x1 :: t1, Nil, _) => mergeSortR(x1 :: acc, t1, Nil, k-1)
        case (Nil, x2 :: t2, _) => mergeSortR(x2 :: acc, Nil, t2, k-1)
        case (Nil, Nil, _) => acc
      }
    }
    mergeSortR(Nil, v1, v2, k).reverse
  }
}
