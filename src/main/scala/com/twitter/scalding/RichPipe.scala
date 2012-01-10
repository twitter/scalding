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
import cascading.pipe.cogroup._
import cascading.flow._
import cascading.operation._
import cascading.operation.aggregator._
import cascading.operation.filter._
import cascading.tuple._
import cascading.cascade._

object RichPipe extends FieldConversions with TupleConversions {
  private var nextPipe = -1

  def apply(p : Pipe) = new RichPipe(p)

  implicit def pipeToRichPipe(pipe : Pipe) : RichPipe = new RichPipe(pipe)

  def getNextName = {
    nextPipe = nextPipe + 1
    "_pipe_" + nextPipe.toString
  }
}

@serializable
class RichPipe(val pipe : Pipe) {
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
   * Use at your own risk.
   */
  def crossWithTiny(tiny : Pipe) = {
    // This should be larger than the number of
    // of reducers but there is no need for it
    // to be much larger.
    val PARALLELISM = 1000
    val tinyJoin = tiny.flatMap(() -> 'joinTiny) { (u:Unit) => (0 until PARALLELISM) }
    //Now attach a random item:
    map(() -> 'joinBig) { (u:Unit) => (new java.util.Random).nextInt(PARALLELISM) }
      .join('joinBig -> 'joinTiny, tinyJoin)
      .discard('joinBig, 'joinTiny)
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
  * Merge or Concatenate several pipes together with this one, but do so while grouping
  * over some set of fields.  Within this group, you can do the usual
  * every/buffer operations.
  *
  * Eventually you must groupBy before writing or joining
  * TODO: In principle, both issues are probably fixable.
  */
  def ++(that : MergedRichPipe) = new MergedRichPipe(this :: that.pipes)
  def ++(that : RichPipe) = new MergedRichPipe(List(this,that))

  // Group all tuples down to one reducer.
  // (due to cascading limitation).
  // This is probably only useful just before setting a tail such as Database
  // tail, so that only one reducer talks to the DB.  Kind of a hack.
  def groupAll : Pipe = groupAll { g => g.takeWhile(0)((t : TupleEntry) => true) }

  // WARNING! this kills parallelism.  All the work is sent to one reducer.
  // Only use this in the case that you truly need all the data on one
  // reducer.
  // Just about the only reasonable case of this data is to reduce all values of a column
  // or count all the rows.
  def groupAll(gs : GroupBuilder => GroupBuilder) = {
    map(()->'__groupAll__) { (u:Unit) => 1 }
    .groupBy('__groupAll__)(gs)
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

  def filter[A:TupleConverter](f : Fields)(fn : (A) => Boolean) : Pipe = {
    implicitly[TupleConverter[A]].assertArityMatches(f)
    new Each(pipe, f, new FilterFunction(convertMapFn[A,Boolean](fn)))
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
      val mf = new MapFunction[T](convertMapFn(fn), fs._2)(setter)
      new Each(pipe, fs._1, mf, defaultMode(fs._1, fs._2))
  }
  def mapTo[A,T](fs : (Fields,Fields))(fn : A => T)
                (implicit conv : TupleConverter[A], setter : TupleSetter[T]) : Pipe = {
      conv.assertArityMatches(fs._1)
      setter.assertArityMatches(fs._2)
      val mf = new MapFunction[T](convertMapFn(fn), fs._2)(setter)
      new Each(pipe, fs._1, mf, Fields.RESULTS)
  }
  def flatMap[A,T](fs : (Fields,Fields))(fn : A => Iterable[T])
                (implicit conv : TupleConverter[A], setter : TupleSetter[T]) : Pipe = {
      conv.assertArityMatches(fs._1)
      setter.assertArityMatches(fs._2)
      val mf = new FlatMapFunction[T](convertMapFn(fn), fs._2)(setter)
      new Each(pipe, fs._1, mf, defaultMode(fs._1,fs._2))
  }
  def flatMapTo[A,T](fs : (Fields,Fields))(fn : A => Iterable[T])
                (implicit conv : TupleConverter[A], setter : TupleSetter[T]) : Pipe = {
      conv.assertArityMatches(fs._1)
      setter.assertArityMatches(fs._2)
      val mf = new FlatMapFunction[T](convertMapFn(fn), fs._2)(setter)
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

  /**
  * Avoid going crazy adding more explicit join modes.  Instead do for some other join
  * mode with a larger pipe:
  * .then { pipe => other.
  *           joinWithSmaller(('other1, 'other2)->('this1, 'this2), pipe, new FancyJoin)
  *       }
  */
  def joinWithSmaller(fs :(Fields,Fields), that : Pipe, joiner : Joiner = new InnerJoin) = {
    //Rename these pipes to avoid cascading name conflicts
    new CoGroup(new Pipe(getNextName, pipe), fs._1, new Pipe(getNextName, that), fs._2, joiner)
  }

  def joinWithLarger(fs : (Fields, Fields), that : Pipe) = {
    that.joinWithSmaller((fs._2, fs._1), this.pipe)
  }

  def leftJoinWithSmaller(fs :(Fields,Fields), that : Pipe) = {
    joinWithSmaller(fs, that, new LeftJoin)
  }

  def leftJoinWithLarger(fs :(Fields,Fields), that : Pipe) = {
    //We swap the order, and turn left into right:
    that.joinWithSmaller((fs._2, fs._1), this.pipe, new RightJoin)
  }

  @deprecated("Equivalent to leftJoinWithSmaller. Be explicit.")
  def leftJoin(field_def :(Fields,Fields), that : Pipe) = join(field_def, that, new LeftJoin)

  @deprecated("Equivalent to joinWithSmaller. Be explicit.")
  def outerJoin(field_def :(Fields,Fields), that : Pipe) = join(field_def, that, new OuterJoin)

  def write(outsource : Source)(implicit flowDef : FlowDef, mode : Mode) = {
    outsource.write(pipe)(flowDef, mode)
    pipe
  }
}

/**
* Represents more than one pipe that have been concatenated, or merged,
* You can only call flatMap/map/filter/group/write on this set.
* This is exactly a Monad situation, but we haven't abstracted so it is full
* of boilerplate.
*/
class MergedRichPipe(val pipes : List[RichPipe]) {
  //Get the implicit conversions:
  import RichPipe._

  def ++(that : MergedRichPipe) = new MergedRichPipe(that.pipes ++ pipes)
  def ++(that : RichPipe) = new MergedRichPipe(that :: pipes)

  def rename(fields : (Fields,Fields)) = {
    new MergedRichPipe(pipes.map { (rp : RichPipe) => rp.rename(fields) }.map { RichPipe(_) })
  }

  def project(f : Any*) = {
    new MergedRichPipe(pipes.map { (rp : RichPipe) => rp.project(f) }.map { RichPipe(_) })
  }

  def discard(f : Any*) = {
    new MergedRichPipe(pipes.map { (rp : RichPipe) => rp.discard(f) }.map { RichPipe(_) })
  }

  //Insert a function into the pipeline:
  def then[T,U](pfn : (T) => U)(implicit in : (RichPipe)=>T, out : (U)=>Pipe) : MergedRichPipe = {
    new MergedRichPipe(pipes.map { rp => out(pfn(in(rp))) }.map { RichPipe(_) } )
  }

  def filter[A:TupleConverter](f : Any*)(fn : (A) => Boolean) : MergedRichPipe = {
    new MergedRichPipe(pipes.map { (rp : RichPipe) => rp.filter(f)(fn) }
                            .map { RichPipe(_) })
  }

  def map[A,T](fs : (Fields,Fields))(fn : A => T)
                (implicit conv : TupleConverter[A], setter : TupleSetter[T]) = {
    new MergedRichPipe(pipes.map { _.map[A,T](fs)(fn)(conv,setter) }.map { RichPipe(_) })
  }
  def mapTo[A,T](fs : (Fields,Fields))(fn : A => T)
                (implicit conv : TupleConverter[A], setter : TupleSetter[T]) = {
    new MergedRichPipe(pipes.map { _.mapTo[A,T](fs)(fn)(conv,setter) }.map { RichPipe(_) })
  }
  def flatMap[A,T](fs : (Fields,Fields))(fn : A => Iterable[T])
                (implicit conv : TupleConverter[A], setter : TupleSetter[T]) = {
    new MergedRichPipe(pipes.map { _.flatMap[A,T](fs)(fn)(conv,setter) }.map { RichPipe(_) })
  }
  def flatMapTo[A,T](fs : (Fields,Fields))(fn : A => Iterable[T])
                (implicit conv : TupleConverter[A], setter : TupleSetter[T]) = {
    new MergedRichPipe(pipes.map { _.flatMapTo[A,T](fs)(fn)(conv,setter) }.map { RichPipe(_) })
  }

  def groupBy(f : Any*)(gs : GroupBuilder => GroupBuilder) : Pipe = {
    val mpipes = pipes.map { rp : RichPipe => new Pipe(getNextName, rp.pipe) }
    gs(new GroupBuilder(f)).schedule(pipes.head.pipe.getName, mpipes : _*)
  }

  def unique(f : Any*) : Pipe = groupBy(f : _*){g => g}

  // TODO: I think we can handle CoGroup here, but we need to look
}
