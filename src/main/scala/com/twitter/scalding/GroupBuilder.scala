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

import cascading.pipe._
import cascading.pipe.assembly._
import cascading.operation._
import cascading.operation.aggregator._
import cascading.operation.filter._
import cascading.tuple.Fields
import cascading.tuple.{Tuple => CTuple, TupleEntry}

import com.twitter.scalding.mathematics.{Monoid, Ring}

import scala.collection.JavaConverters._
import scala.annotation.tailrec
import scala.math.Ordering

// This controls the sequence of reductions that happen inside a
// particular grouping operation.  Not all elements can be combined,
// for instance, a scanLeft/foldLeft generally requires a sorting
// but such sorts are (at least for now) incompatible with doing a combine
// which includes some map-side reductions.
class GroupBuilder(val groupFields : Fields) extends java.io.Serializable {
  // We need the implicit conversions from symbols to Fields
  import Dsl._
  /**
  * Holds the "reducers/combiners", the things that we can do paritially map-side.
  */
  private var reds : Option[List[AggregateBy]] = Some(Nil)

  /**
  * This is the description of this Grouping in terms of a sequence of Every operations
  */
  protected var evs : List[Pipe => Every] = Nil
  protected var isReversed : Boolean = false
  protected var sortBy : Option[Fields] = None
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
  /**
   * Override the number of reducers used in the groupBy.
   */
  def reducers(r : Int) = {
    if(r > 0) {
      numReducers = Some(r)
    }
    this
  }

  // This cancels map side aggregation
  // and forces everything to the reducers
  def forceToReducers = {
    reds = None
    this
  }

  protected def overrideReducers(p : Pipe) : Pipe = {
    numReducers.map { r => RichPipe.setReducers(p, r) }.getOrElse(p)
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
  * Opposite of RichPipe.unpivot.  See SQL/Excel for more on this function
  * converts a row-wise representation into a column-wise one.
  * example: pivot(('feature, 'value) -> ('clicks, 'impressions, 'requests))
  * it will find the feature named "clicks", and put the value in the column with the field named
  * clicks.
  * Absent fields result in null unless a default value is provided. Unnamed output fields are ignored.
  * NOTE: Duplicated fields will result in an error.
  *
  * Hint: if you want more precision, first do a
  * map('value -> value) { x : AnyRef => Option(x) }
  * and you will have non-nulls for all present values, and Nones for values that were present
  * but previously null.  All nulls in the final output will be those truly missing.
  * Similarly, if you want to check if there are any items present that shouldn't be:
  * map('feature -> 'feature) { fname : String =>
  *   if (!goodFeatures(fname)) { throw new Exception("ohnoes") }
  *   else fname
  * }
  */
  def pivot(fieldDef : (Fields, Fields), defaultVal : Any = null) : GroupBuilder = {
    // Make sure the fields are strings:
    mapReduceMap(fieldDef) { pair : (String, AnyRef) =>
      List(pair)
    } { (prev, next) => next ++ prev } // concat into the bigger one
    { outputList =>
      val asMap = outputList.toMap
      assert(asMap.size == outputList.size, "Repeated pivot key fields: " + outputList.toString)
      val values = fieldDef._2
        .iterator.asScala
        // Look up this key:
        .map { fname => asMap.getOrElse(fname.asInstanceOf[String], defaultVal.asInstanceOf[AnyRef]) }
      // Create the cascading tuple
      new CTuple(values.toSeq : _*)
    }
  }

  /**
   * Convert a subset of fields into a list of Tuples. Need to provide the types of the tuple fields.
   * Note that the order of the tuples is not preserved: EVEN IF YOU GroupBuilder.sortBy!
   * If you need ordering use sortedTake or sortBy + scanLeft
   */
  def toList[T](fieldDef : (Fields, Fields))(implicit conv : TupleConverter[T]) : GroupBuilder = {
    val (fromFields, toFields) = fieldDef
    conv.assertArityMatches(fromFields)
    val out_arity = toFields.size
    assert(out_arity == 1, "toList: can only add a single element to the GroupBuilder")
    val reverseAfter = sortBy.isDefined
    mapReduceMap[T, List[T], List[T]](fieldDef) { //Map
      // TODO this is questionable, how do you get a list including nulls?
      x => if (null != x) List(x) else Nil
    } { //Reduce, note the bigger list is likely on the left, so concat into it:
      (prev, current) => current ++ prev
    } {
      /*
       * There are two cases:
       * 1) there has been no sortBy called, in which case, order does not matter.
       * 2) sortBy has been called, so we used a GroupBy to push everything to the reducers
       *    and as such, the list is now left in reverse ordered state.  If sortBy has been called
       *    we should reverse at this stage:
       */
      t => if (reverseAfter) t.reverse else t
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
    mapStream[CTuple,CTuple](Fields.VALUES -> Fields.ARGS){ s =>
      s.drop(cnt)
    }(CTupleConverter, CascadingTupleSetter)
  }
  //Drop while the predicate is true, starting at the first false, output all
  def dropWhile[T](f : Fields)(fn : (T) => Boolean)(implicit conv : TupleConverter[T]) : GroupBuilder = {
    mapStream[TupleEntry,CTuple](f -> Fields.ARGS){ s =>
      s.dropWhile(te => fn(conv(te))).map { _.getTuple }
    }(TupleEntryConverter, CascadingTupleSetter)
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
   *  BEST PRACTICE: make sure init is an immutable object.
   *  NOTE: init needs to be serializable with Kryo (because we copy it for each
   *    grouping to avoid possible errors using a mutable init object).
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
  def head(fd : (Fields,Fields)) : GroupBuilder = {
    //CTuple's have unknown arity so we have to put them into a Tuple1 in the middle phase:
    mapReduceMap(fd) { ctuple : CTuple => Tuple1(ctuple) }
      { (oldVal, newVal) => oldVal }
      { result => result._1 }
  }
  def head(f : Symbol*) : GroupBuilder = head(f -> f)

  def last(fd : (Fields,Fields)) = {
    //CTuple's have unknown arity so we have to put them into a Tuple1 in the middle phase:
    mapReduceMap(fd) { ctuple : CTuple => Tuple1(ctuple) }
      { (oldVal, newVal) => newVal }
      { result => result._1 }
  }
  def last(f : Symbol*) : GroupBuilder = last(f -> f)

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
  *
  * The previous output goes into the reduce function on the left, like foldLeft,
  * so if your operation is faster for the accumulator to be on one side, be aware.
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
    assert(middleSetter.arity > 0,
      "The middle arity must have definite size, try wrapping in scala.Tuple1 if you need a hack")
    // Create the required number of middlefields based on the arity of middleSetter
    val middleFields = strFields( Range(0, middleSetter.arity).map{i => getNextMiddlefield} )
    val mrmBy = new MRMBy[T,X,U](fromFields, middleFields, toFields,
      mapfn, redfn, mapfn2, startConv, middleSetter, middleConv, endSetter)
    tryAggregateBy(mrmBy, ev)
    this
  }

  /** Corresponds to a Cascading Buffer
   * which allows you to stream through the data, keeping some, dropping, scanning, etc...
   * The iterator you are passed is lazy, and mapping will not trigger the
   * entire evaluation.  If you convert to a list (i.e. to reverse), you need to be aware
   * that memory constraints may become an issue.
   *
   * WARNING: Any fields not referenced by the input fields will be aligned to the first output,
   * and the final hadoop stream will have a length of the maximum of the output of this, and
   * the input stream.  So, if you change the length of your inputs, the other fields won't
   * be aligned.  YOU NEED TO INCLUDE ALL THE FIELDS YOU WANT TO KEEP ALIGNED IN THIS MAPPING!
   * POB: This appears to be a Cascading design decision.
   *
   * WARNING: mapfn needs to be stateless.  Multiple calls needs to be safe (no mutable
   * state captured)
   */
  def mapStream[T,X](fieldDef : (Fields,Fields))(mapfn : (Iterator[T]) => TraversableOnce[X])
    (implicit conv : TupleConverter[T], setter : TupleSetter[X]) = {
    val (inFields, outFields) = fieldDef
    //Check arity
    conv.assertArityMatches(inFields)
    setter.assertArityMatches(outFields)
    val b = new BufferOp[Unit,T,X]((),
      (u : Unit, it: Iterator[T]) => mapfn(it), outFields, conv, setter)
    every(pipe => new Every(pipe, inFields, b, defaultMode(inFields, outFields)))
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
     * if we are not sorting, we don't care about order.  If we are, we need to reverse the list at
     * the end.
     */
    val reverseAfter = sortBy.isDefined
    mapReduceMap(fieldDef) { (x : String) => List(x) }
      { (prev, next) => next ++ prev } // reversing the order to keep the bigger list on the right
      { resultList =>
        (if (reverseAfter) resultList.reverse else resultList)
          .mkString(start, sep, end)
      }
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

  /**
   * apply an associative/commutative operation on the left field.
   * Example: reduce(('mass,'allids)->('totalMass, 'idset)) { (left:(Double,Set[Long]),right:(Double,Set[Long])) =>
   *   (left._1 + right._1, left._2 ++ right._2)
   * }
   * Equivalent to a mapReduceMap with trivial (identity) map functions.
   *
   * The previous output goes into the reduce function on the left, like foldLeft,
   * so if your operation is faster for the accumulator to be on one side, be aware.
   */
  def reduce[T](fieldDef : (Fields, Fields))(fn : (T,T)=>T)
               (implicit setter : TupleSetter[T], conv : TupleConverter[T]) : GroupBuilder = {
    mapReduceMap[T,T,T](fieldDef)({ t => t })(fn)({t => t})(conv,setter,conv,setter)
  }
  //Same as reduce(f->f)
  def reduce[T](fieldDef : Symbol*)(fn : (T,T)=>T)(implicit setter : TupleSetter[T],
                                 conv : TupleConverter[T]) : GroupBuilder = {
    reduce(fieldDef -> fieldDef)(fn)(setter,conv)
  }

  // Abstract algebra reductions (plus, times, dot):

  /** use Monoid.plus to compute a sum.  Not called sum to avoid conflicting with standard sum
   * Your Monoid[T] should be associated and commutative, else this doesn't make sense
   */
  def plus[T](fd : (Fields,Fields))
    (implicit monoid : Monoid[T], tconv : TupleConverter[T], tset : TupleSetter[T]) : GroupBuilder = {
    // We reverse the order because the left is the old value in reduce, and for list concat
    // we are much better off concatenating into the bigger list
    reduce[T](fd)({ (left, right) => monoid.plus(right, left) })(tset, tconv)
  }

  // The same as plus(fs -> fs)
  def plus[T](fs : Symbol*)
    (implicit monoid : Monoid[T], tconv : TupleConverter[T], tset : TupleSetter[T]) : GroupBuilder = {
    plus[T](fs -> fs)(monoid,tconv,tset)
  }

  // Returns the product of all the items in this grouping
  def times[T](fd : (Fields,Fields))
    (implicit ring : Ring[T], tconv : TupleConverter[T], tset : TupleSetter[T]) : GroupBuilder = {
    // We reverse the order because the left is the old value in reduce, and for list concat
    // we are much better off concatenating into the bigger list
    reduce[T](fd)({ (left, right) => ring.times(right, left) })(tset, tconv)
  }

  // The same as times(fs -> fs)
  def times[T](fs : Symbol*)
    (implicit ring : Ring[T], tconv : TupleConverter[T], tset : TupleSetter[T]) : GroupBuilder = {
    times[T](fs -> fs)(ring,tconv,tset)
  }

  // First do "times" on each pair, then "plus" them all together.
  // Example: groupBy('x) { _.dot('y,'z, 'ydotz) }
  def dot[T](left : Fields, right : Fields, result : Fields)
    (implicit ttconv : TupleConverter[Tuple2[T,T]], ring : Ring[T],
     tconv : TupleConverter[T], tset : TupleSetter[T]) : GroupBuilder = {
    mapReduceMap[(T,T),T,T](Fields.merge(left, right) -> result) { init : (T,T) =>
      ring.times(init._1, init._2)
    } { (left : T, right: T) =>
      ring.plus(left, right)
    } { result => result }
  }

  def reverse : GroupBuilder = {
    assert(reds.isEmpty, "Cannot sort when reducing")
    assert(!isReversed, "Reverse called a second time! Only one allowed")
    isReversed = true
    this
  }

  /** analog of standard scanLeft (@see scala.collection.Iterable.scanLeft )
   * This invalidates map-side aggregation, forces all data to be transferred
   * to reducers.  Use only if you REALLY have to.
   *
   *  BEST PRACTICE: make sure init is an immutable object.
   *  NOTE: init needs to be serializable with Kryo (because we copy it for each
   *    grouping to avoid possible errors using a mutable init object).
   */
  def scanLeft[X,T](fieldDef : (Fields,Fields))(init : X)(fn : (X,T) => X)
                 (implicit setter : TupleSetter[X], conv : TupleConverter[T]) : GroupBuilder = {
    val (inFields, outFields) = fieldDef
    //Check arity
    conv.assertArityMatches(inFields)
    setter.assertArityMatches(outFields)
    val b = new BufferOp[X,T,X](init,
      // On scala 2.8, there is no scanLeft
      // On scala 2.9, their implementation creates an off-by-one bug with the unused fields
      (i : X, it: Iterator[T]) => new ScanLeftIterator(it, i, fn),
      outFields, conv, setter)
    every(pipe => new Every(pipe, inFields, b, defaultMode(inFields, outFields)))
  }

  def groupMode : GroupMode = {
    return reds match {
      case None => GroupByMode
      case Some(Nil) => IdentityMode
      case Some(redList) => AggregateByMode
    }
  }

  def schedule(name : String, pipe : Pipe) : Pipe = {

    groupMode match {
      //In this case we cannot aggregate, so group:
      case GroupByMode => {
        val startPipe : Pipe = sortBy match {
          case None => new GroupBy(name, pipe, groupFields)
          case Some(sf) => new GroupBy(name, pipe, groupFields, sf, isReversed)
        }
        overrideReducers(startPipe)

        // Time to schedule the addEverys:
        evs.foldRight(startPipe)( (op : Pipe => Every, p) => op(p) )
      }
      //This is the case where the group function is identity: { g => g }
      case IdentityMode => {
        val gb = new GroupBy(name, pipe, groupFields)
        overrideReducers(gb)
        gb
      }
      //There is some non-empty AggregateBy to do:
      case AggregateByMode => {
        val redlist = reds.get
        val THRESHOLD = 100000 //tune this, default is 10k
        val ag = new AggregateBy(name, pipe, groupFields,
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
  def size(thisF : Fields) : GroupBuilder = {
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
    mapStream[CTuple,CTuple](Fields.VALUES -> Fields.ARGS){ s =>
      s.take(cnt)
    }(CTupleConverter, CascadingTupleSetter)
  }
  //Take while the predicate is true, starting at the first false, output all
  def takeWhile[T](f : Fields)(fn : (T) => Boolean)(implicit conv : TupleConverter[T]) : GroupBuilder = {
    mapStream[TupleEntry,CTuple](f -> Fields.ARGS){ s =>
      s.takeWhile(te => fn(conv(te))).map { _.getTuple }
    }(TupleEntryConverter, CascadingTupleSetter)
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

/** Scala 2.8 Iterators don't support scanLeft so we have to reimplement
 */
class ScanLeftIterator[T,U](it : Iterator[T], init : U, fn : (U,T) => U) extends Iterator[U] with java.io.Serializable {
  protected var prev : Option[U] = None
  def hasNext : Boolean = { prev.isEmpty || it.hasNext }
  def next = {
    prev = prev.map { fn(_, it.next) }
            .orElse(Some(init))
    prev.get
  }
}

sealed private[scalding] abstract class GroupMode
private[scalding] case object AggregateByMode extends GroupMode
private[scalding] case object GroupByMode extends GroupMode
private[scalding] case object IdentityMode extends GroupMode
