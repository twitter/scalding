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

import cascading.tap._
import cascading.scheme._
import cascading.pipe._
import cascading.pipe.assembly._
import cascading.pipe.joiner._
import cascading.flow._
import cascading.operation._
import cascading.operation.aggregator._
import cascading.operation.filter._
import cascading.tuple._
import cascading.cascade._

object RichPipe extends FieldConversions with TupleConversions with java.io.Serializable {
  private var nextPipe = -1

  def apply(p : Pipe) = new RichPipe(p)

  implicit def pipeToRichPipe(pipe : Pipe) : RichPipe = new RichPipe(pipe)

  def getNextName = {
    nextPipe = nextPipe + 1
    "_pipe_" + nextPipe.toString
  }

  def assignName(p : Pipe) = new Pipe(getNextName, p)
}

class RichPipe(val pipe : Pipe) extends java.io.Serializable {
  import RichPipe._

  // Rename the current pipe
  def name(s : String) = new Pipe(s, pipe)

  //Keep only the given fields, and discard the rest.
  //takes any number of parameters as long as we can convert
  //them to a fields object
  def project(fields : Fields) = {
    new Each(pipe, fields, new Identity(fields))
  }

  /*
   * WARNING! doing a cross product with even a moderate sized pipe can
   * create ENORMOUS output.  The use-case here is attaching a constant (e.g.
   * a number or a dictionary or set) to each row in another pipe.
   * A common use-case comes from a groupAll and reduction to one row,
   * then you want to send the results back out to every element in a pipe
   *
   * This uses joinWithTiny, so tiny pipe is replicated to all Mappers.  If it
   * is large, this will blow up.  Get it: be foolish here and LOSE IT ALL!
   *
   * Use at your own risk.
   */
  def crossWithTiny(tiny : Pipe) = {
    val tinyJoin = tiny.map(() -> '__joinTiny__) { (u:Unit) => 1 }
    map(() -> '__joinBig__) { (u:Unit) => 1 }
      .joinWithTiny('__joinBig__ -> '__joinTiny__, tinyJoin)
      .discard('__joinBig__, '__joinTiny__)
  }

  //Discard the given fields, and keep the rest
  //Kind of the opposite previous.
  def discard(f : Fields) = new Each(pipe, f, new NoOp, Fields.SWAP)

  //Insert a function into the pipeline:
  def then[T,U](pfn : (T) => U)(implicit in : (RichPipe)=>T, out : (U)=>Pipe) = out(pfn(in(this)))

  //
  // group
  //
  // builder is typically a block that modifies the given GroupBuilder
  // the final OUTPUT of the block is used to schedule the new pipe
  // each method in GroupBuilder returns this, so it is recommended
  // to chain them and use the default input:
  //   _.size.max('f1) etc...
  def groupBy(f : Fields)(builder : GroupBuilder => GroupBuilder) : Pipe = {
    builder(new GroupBuilder(f)).schedule(pipe.getName, pipe)
  }

  @deprecated("Use groupBy for more consistency with scala.collections API")
  def group(f : Fields)(builder : GroupBuilder => GroupBuilder) : Pipe = groupBy(f)(builder)

  // Returns the set of unique tuples containing the specified fields
  def unique(f : Fields) : Pipe = groupBy(f) { _.size('__uniquecount__) }.project(f)

  /**
  * Merge or Concatenate several pipes together with this one:
  */
  def ++(that : Pipe) = new Merge(assignName(this.pipe), assignName(that))

  // Group all tuples down to one reducer.
  // (due to cascading limitation).
  // This is probably only useful just before setting a tail such as Database
  // tail, so that only one reducer talks to the DB.  Kind of a hack.
  def groupAll : Pipe = groupAll { g =>
    g.takeWhile(0)((t : TupleEntry) => true)
  }

  // WARNING! this kills parallelism.  All the work is sent to one reducer.
  // Only use this in the case that you truly need all the data on one
  // reducer.
  // Just about the only reasonable case of this data is to reduce all values of a column
  // or count all the rows.
  def groupAll(gs : GroupBuilder => GroupBuilder) = {
    map(()->'__groupAll__) { (u:Unit) => 1 }
    .groupBy('__groupAll__) { gs(_).reducers(1) }
    .discard('__groupAll__)
  }

  /**
  * Rename some set of N fields as another set of N fields
  * usage: rename('x -> 'z)
  *        rename(('x,'y) -> ('X,'Y))
  * WARNING: rename('x,'y) is interpreted by scala as rename(Tuple2('x,'y))
  * which then does rename('x -> 'y).  This is probably not what is intended
  * but the compiler doesn't resolve the ambiguity.  YOU MUST CALL THIS WITH
  * A TUPLE2!!!!!  If you don't, expect the unexpected.
  */
  def rename(fields : (Fields,Fields)) : Pipe = {
    val (fromFields, toFields) = fields
    val in_arity = fromFields.size
    val out_arity = toFields.size
    assert(in_arity == out_arity, "Number of field names must match for rename")
    new Each(pipe, fromFields, new Identity( toFields ), Fields.SWAP)
  }

  def filter[A](f : Fields)(fn : (A) => Boolean)
      (implicit conv : TupleConverter[A]) : Pipe = {
    conv.assertArityMatches(f)
    new Each(pipe, f, new FilterFunction(fn, conv))
  }

  // If you use a map function that does not accept TupleEntry args,
  // which is the common case, an implicit conversion in GeneratedConversions
  // will convert your function into a (TupleEntry => T).  The result type
  // T is converted to a cascading Tuple by an implicit TupleSetter[T].
  // acceptable T types are primitive types, cascading Tuples of those types,
  // or scala.Tuple(1-22) of those types.
  //
  // After the map, the input arguments will be set to the output of the map,
  // so following with filter or map is fine without a new using statement if
  // you mean to operate on the output.
  //
  // map('data -> 'stuff)
  //   * if output equals input, REPLACE is used.
  //   * if output or input is a subset of the other SWAP is used.
  //   * otherwise we append the new fields (cascading Fields.ALL is used)
  //
  // mapTo('data -> 'stuff)
  //   Only the results (stuff) are kept (cascading Fields.RESULTS)
  //
  // Note: Using mapTo is the same as using map followed by a project for
  // selecting just the ouput fields
  def map[A,T](fs : (Fields,Fields))(fn : A => T)
                (implicit conv : TupleConverter[A], setter : TupleSetter[T]) : Pipe = {
      conv.assertArityMatches(fs._1)
      setter.assertArityMatches(fs._2)
      val mf = new MapFunction[A,T](fn, fs._2, conv, setter)
      new Each(pipe, fs._1, mf, defaultMode(fs._1, fs._2))
  }
  def mapTo[A,T](fs : (Fields,Fields))(fn : A => T)
                (implicit conv : TupleConverter[A], setter : TupleSetter[T]) : Pipe = {
      conv.assertArityMatches(fs._1)
      setter.assertArityMatches(fs._2)
      val mf = new MapFunction[A,T](fn, fs._2, conv, setter)
      new Each(pipe, fs._1, mf, Fields.RESULTS)
  }
  def flatMap[A,T](fs : (Fields,Fields))(fn : A => Iterable[T])
                (implicit conv : TupleConverter[A], setter : TupleSetter[T]) : Pipe = {
      conv.assertArityMatches(fs._1)
      setter.assertArityMatches(fs._2)
      val mf = new FlatMapFunction[A,T](fn, fs._2, conv, setter)
      new Each(pipe, fs._1, mf, defaultMode(fs._1,fs._2))
  }
  def flatMapTo[A,T](fs : (Fields,Fields))(fn : A => Iterable[T])
                (implicit conv : TupleConverter[A], setter : TupleSetter[T]) : Pipe = {
      conv.assertArityMatches(fs._1)
      setter.assertArityMatches(fs._2)
      val mf = new FlatMapFunction[A,T](fn, fs._2, conv, setter)
      new Each(pipe, fs._1, mf, Fields.RESULTS)
  }

  // Keep at most n elements.  This is implemented by keeping
  // approximately n/k elements on each of the k mappers or reducers (whichever we wind
  // up being scheduled on).
  def limit(n : Long) = new Each(pipe, new Limit(n))

  def debug = new Each(pipe, new Debug())

  @deprecated("Equivalent to joinWithSmaller. Be explicit.")
  def join(fs :(Fields,Fields), that : Pipe, joiner : Joiner = new InnerJoin) = {
    joinWithSmaller(fs, that, joiner)
  }

  private val REDUCER_KEY = "mapred.reduce.tasks"

  /**
  * Avoid going crazy adding more explicit join modes.  Instead do for some other join
  * mode with a larger pipe:
  * .then { pipe => other.
  *           joinWithSmaller(('other1, 'other2)->('this1, 'this2), pipe, new FancyJoin)
  *       }
  */
  def joinWithSmaller(fs :(Fields,Fields), that : Pipe, joiner : Joiner = new InnerJoin, reducers : Int = -1) = {
    //Rename these pipes to avoid cascading name conflicts
    val p = new CoGroup(assignName(pipe), fs._1, assignName(that), fs._2, joiner)
    if(reducers > 0) {
      p.getProcessConfigDef()
        .setProperty(REDUCER_KEY, reducers.toString)
    } else if(reducers != -1) {
      throw new IllegalArgumentException("Number of reducers must be non-negative")
    }
    p
  }

  def joinWithLarger(fs : (Fields, Fields), that : Pipe, joiner : Joiner = new InnerJoin, reducers : Int = -1) = {
    that.joinWithSmaller((fs._2, fs._1), this.pipe, joiner, reducers)
  }

  def leftJoinWithSmaller(fs :(Fields,Fields), that : Pipe, reducers : Int = -1) = {
    joinWithSmaller(fs, that, new LeftJoin, reducers)
  }

  def leftJoinWithLarger(fs :(Fields,Fields), that : Pipe, reducers : Int = -1) = {
    //We swap the order, and turn left into right:
    that.joinWithSmaller((fs._2, fs._1), this.pipe, new RightJoin, reducers)
  }

  @deprecated("Equivalent to leftJoinWithSmaller. Be explicit.")
  def leftJoin(field_def :(Fields,Fields), that : Pipe) = join(field_def, that, new LeftJoin)

  @deprecated("Equivalent to joinWithSmaller. Be explicit.")
  def outerJoin(field_def :(Fields,Fields), that : Pipe) = join(field_def, that, new OuterJoin)

  /**
   * This does an assymmetric join, using cascading's "Join".  This only runs through
   * this pipe once, and keeps the right hand side pipe in memory (but is spillable).
   * WARNING: this does not work with outer joins, or right joins (according to
   * cascading documentation), only inner and left join versions are given.
   */
  def joinWithTiny(fs :(Fields,Fields), that : Pipe) = {
    //Rename these pipes to avoid cascading name conflicts
    new Join(assignName(pipe), fs._1, assignName(that), fs._2, new InnerJoin)
  }

  def leftJoinWithTiny(fs :(Fields,Fields), that : Pipe) = {
    //Rename these pipes to avoid cascading name conflicts
    new Join(assignName(pipe), fs._1, assignName(that), fs._2, new LeftJoin)
  }

  def write(outsource : Source)(implicit flowDef : FlowDef, mode : Mode) = {
    outsource.write(pipe)(flowDef, mode)
    pipe
  }

  def normalize(f : Symbol) : Pipe = {
    val total = groupAll { _.sum(f -> 'total_for_normalize) }
    crossWithTiny(total)
    .map((f, 'total_for_normalize) -> f) { args : (Double, Double) =>
      args._1 / args._2
    }
  }
}
