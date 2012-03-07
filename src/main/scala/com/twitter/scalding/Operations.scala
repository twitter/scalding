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
package com.twitter.scalding {

import cascading.operation._
import cascading.tuple._
import cascading.flow._
import cascading.pipe.assembly.AggregateBy
import cascading.pipe._

import scala.collection.JavaConverters._

  class FlatMapFunction[T](fn : (TupleEntry) => Iterable[T], fields : Fields, conv : TupleSetter[T])
    extends BaseOperation[Any](fields) with Function[Any] {

    def operate(flowProcess : FlowProcess[_], functionCall : FunctionCall[Any]) {
      fn(functionCall.getArguments).foreach { arg : T =>
        val this_tup = conv(arg)
        functionCall.getOutputCollector.add(this_tup)
      }
    }
  }

  class MapFunction[T](fn : (TupleEntry) => T, fields : Fields, conv : TupleSetter[T])
    extends BaseOperation[Any](fields) with Function[Any] {

    def operate(flowProcess : FlowProcess[_], functionCall : FunctionCall[Any]) {
      val res = fn(functionCall.getArguments)
      functionCall.getOutputCollector.add(conv(res))
    }
  }

  class FilterFunction(fn : (TupleEntry => Boolean)) extends BaseOperation[Any] with Filter[Any] {
    def isRemove(flowProcess : FlowProcess[_], filterCall : FilterCall[Any]) : Boolean = !fn(filterCall.getArguments)
  }

  class FoldAggregator[X](fn : (X,TupleEntry) => X, init : X, fields : Fields, conv : TupleSetter[X])
    extends BaseOperation[X](fields) with Aggregator[X] {

    def start(flowProcess : FlowProcess[_], call : AggregatorCall[X]) {
      call.setContext(init)
    }

    def aggregate(flowProcess : FlowProcess[_], call : AggregatorCall[X]) {
      call.setContext(fn(call.getContext, call.getArguments))
    }

    def complete(flowProcess : FlowProcess[_], call : AggregatorCall[X]) {
      emit(flowProcess, call)
    }

    def emit(flowProcess : FlowProcess[_], call : AggregatorCall[X]) {
      call.getOutputCollector.add(conv(call.getContext))
    }
  }

  /*
   * fields are the declared fields of this aggregator
   */
  class MRMAggregator[X,U](fsmf : TupleEntry => X, rfn : (X,X) => X, mrfn : X => U, conv : TupleSetter[U], fields : Fields)
    extends BaseOperation[Option[X]](fields) with Aggregator[Option[X]] {

    def start(flowProcess : FlowProcess[_], call : AggregatorCall[Option[X]]) {
        call.setContext(None)
    }

    def extractArgument(call : AggregatorCall[Option[X]]) : X = fsmf(call.getArguments)

    def aggregate(flowProcess : FlowProcess[_], call : AggregatorCall[Option[X]]) {
      val arg = extractArgument(call)
      call.getContext match {
        case Some(v) =>  call.setContext(Some(rfn(v,arg)))
        case None => call.setContext(Some(arg))
      }
    }

    def complete(flowProcess : FlowProcess[_], call : AggregatorCall[Option[X]]) {
      call.getContext match {
        case Some(v) => call.getOutputCollector.add(conv(mrfn(v)))
        case None => throw new Exception("MRMAggregator completed without any args")
      }
    }
  }

  /**
   * This handles the mapReduceMap and MkString work on the map-side of the operation.  The code below
   * attempts to be optimal with respect to memory allocations and performance, not functional
   * style purity.
   */
  abstract class FoldFunctor[X](fields : Fields) extends AggregateBy.Functor {

    // Extend these three methods:
    def first(args : TupleEntry) : X
    def subsequent(oldValue : X, newArgs : TupleEntry) : X
    def finish(lastValue : X) : Tuple

    override final def getDeclaredFields = fields

    /*
     * It's important to keep all state in the context as Cascading seems to
     * reuse these objects, so any per instance state might give unexpected
     * results.
     */
    override final def aggregate(flowProcess : FlowProcess[_], args : TupleEntry, context : Tuple) = {
      var nextContext : Tuple = null
      val newContextObj = if (null == context) {
        // First call, make a new mutable tuple to reduce allocations:
        nextContext = Tuple.size(1)
        first(args)
      }
      else {
        //We are updating
        val oldValue = context.getObject(0).asInstanceOf[X]
        nextContext = context
        subsequent(oldValue, args)
      }
      nextContext.set(0, newContextObj.asInstanceOf[AnyRef])
      //Return context for reuse next time:
      nextContext
    }

    override final def complete(flowProcess : FlowProcess[_], context : Tuple) = {
      if (null == context) {
        throw new Exception("FoldFunctor completed with any aggregate calls")
      }
      else {
        finish(context.getObject(0).asInstanceOf[X])
      }
    }
  }

  /**
   * This handles the mapReduceMap work on the map-side of the operation.  The code below
   * attempts to be optimal with respect to memory allocations and performance, not functional
   * style purity.
   */
  class MRMFunctor[X](mrfn : TupleEntry => X, rfn : (X, X) => X, conv : TupleSetter[X], fields : Fields)
    extends FoldFunctor[X](fields) {

    override def first(args : TupleEntry) : X = mrfn(args)
    override def subsequent(oldValue : X, newArgs : TupleEntry) = rfn(oldValue, mrfn(newArgs))
    override def finish(lastValue : X) = conv(lastValue)
  }

  /**
   * MapReduceMapBy Class
   */
  class MRMBy[X,U](arguments : Fields,
                   declaredFields : Fields,
                   middleFields : Fields,
                   mfn : TupleEntry => X,
                   rfn : (X,X) => X,
                   mfn2 : X => U,
                   conv : TupleSetter[X],
                   conv2 : TupleSetter[U],
                   uconv : TupleConverter[X]) extends AggregateBy(
        arguments,
        new MRMFunctor[X](mfn, rfn, conv, middleFields),
        new MRMAggregator[X,U](args => uconv.get(args), rfn, mfn2, conv2, declaredFields))

  class CombineBy[X](arguments : Fields, declaredFields : Fields,
                     fn : (X,X) => X, conv : TupleSetter[X],
                     uconv :  TupleConverter[X]) extends AggregateBy(
        arguments,
        new MRMFunctor[X](args => uconv.get(args), fn, conv, declaredFields),
        new MRMAggregator[X,X](args => uconv.get(args), fn, x => x, conv, declaredFields))

  class MkStringFunctor(sep : String, fields : Fields) extends FoldFunctor[List[String]](fields) {

    private def stringOf(args : TupleEntry) = args.getTuple.getString(0)

    // Make the singleton list:
    override def first(args : TupleEntry) = List(stringOf(args))
    // Append to the list:
    override def subsequent(oldValue : List[String], newArgs : TupleEntry) = {
      stringOf(newArgs) :: oldValue
    }
    // Make the string and put it in a Tuple
    override def finish(lastValue : List[String]) = new Tuple(lastValue.mkString(sep))
  }

  class MkStringAggregator(start : String, sep : String, end : String, fields : Fields)
    extends BaseOperation[List[String]](fields) with Aggregator[List[String]] {
      def start(fp : FlowProcess[_], call : AggregatorCall[List[String]]) {
        call.setContext(Nil)
      }
      def aggregate(fp : FlowProcess[_], call : AggregatorCall[List[String]]) {
        call.setContext(call.getArguments.getTuple.getString(0) :: (call.getContext))
      }
      def complete(fp : FlowProcess[_], call : AggregatorCall[List[String]]) {
        call.getOutputCollector.add(new Tuple(call.getContext.mkString(start,sep,end)))
      }
    }

  class MkStringBy(start : String, sep : String, end : String,
                   arguments : Fields, declaredFields : Fields) extends
                   AggregateBy(arguments,
                               new MkStringFunctor(sep, declaredFields),
                               new MkStringAggregator(start, sep, end, declaredFields))


  class ScanBuffer[X](fn : (X,TupleEntry) => X, init : X, fields : Fields,
                      conv : TupleSetter[X])
    extends BaseOperation[Any](fields) with Buffer[Any] {

    def operate(flowProcess : FlowProcess[_], call : BufferCall[Any]) {
      var accum = init
      //TODO: I'm not sure we want to add output for the accumulator, this
      //pollutes the output with nulls where there was not an input row.
      call.getOutputCollector.add(conv(accum))
      call.getArgumentsIterator.asScala.foreach {entry =>
        accum = fn(accum, entry)
        call.getOutputCollector.add(conv(accum))
      }
    }
  }
  class TakeBuffer(cnt : Int) extends BaseOperation[Any](Fields.ARGS) with Buffer[Any] {
    def operate(flowProcess : FlowProcess[_], call : BufferCall[Any]) {
      call.getArgumentsIterator.asScala.take(cnt).foreach {entry =>
        call.getOutputCollector.add(entry)
      }
    }
  }
  class TakeWhileBuffer(fn : (TupleEntry) => Boolean) extends BaseOperation[Any](Fields.ARGS) with Buffer[Any] {
    def operate(flowProcess : FlowProcess[_], call : BufferCall[Any]) {
      call.getArgumentsIterator.asScala.takeWhile(fn).foreach {entry =>
        call.getOutputCollector.add(entry)
      }
    }
  }
  class DropBuffer(cnt : Int) extends BaseOperation[Any](Fields.ARGS) with Buffer[Any] {
    def operate(flowProcess : FlowProcess[_], call : BufferCall[Any]) {
      call.getArgumentsIterator.asScala.drop(cnt).foreach {entry =>
        call.getOutputCollector.add(entry)
      }
    }
  }
  class DropWhileBuffer(fn : (TupleEntry) => Boolean) extends BaseOperation[Any](Fields.ARGS) with Buffer[Any] {
    def operate(flowProcess : FlowProcess[_], call : BufferCall[Any]) {
      call.getArgumentsIterator.asScala.dropWhile(fn).foreach {entry =>
        call.getOutputCollector.add(entry)
      }
    }
  }
  /*
   * fields are the declared fields of this aggregator
   */
  class ExtremumAggregator(choose_max : Boolean, fields : Fields)
    extends BaseOperation[Tuple](fields) with Aggregator[Tuple] {

    def start(flowProcess : FlowProcess[_], call : AggregatorCall[Tuple]) {
        call.setContext(null)
    }
    private def getArgs(call : AggregatorCall[Tuple]) = call.getArguments.getTuple

    def aggregate(flowProcess : FlowProcess[_], call : AggregatorCall[Tuple]) = {
      val arg = getArgs(call)
      val ctx = call.getContext
      if (null == ctx) {
        call.setContext(arg)
      }
      else {
        val (max, min) = if( ctx.compareTo(arg) < 0 ) {
          (arg, ctx)
        } else { (ctx, arg) }
        call.setContext(if(choose_max) max else min)
      }
    }
    def complete(flowProcess : FlowProcess[_], call : AggregatorCall[Tuple]) {
      val ctx = call.getContext
      if (null != ctx) {
        call.getOutputCollector.add(ctx)
      }
      else {
        throw new Exception("ExtremumAggregator called only once")
      }
    }
  }
  class ExtremumFunctor(choose_max : Boolean, fields : Fields) extends AggregateBy.Functor {
    override def getDeclaredFields = fields
    def aggregate(flowProcess : FlowProcess[_], args : TupleEntry, context : Tuple) = {
      val this_tup = args.getTuple
      if(context == null) { this_tup }
      else {
        val (max, min) = if( context.compareTo(this_tup) < 0 ) {
          (this_tup, context)
        } else { (context, this_tup) }
        if(choose_max) max else min
      }
    }
    def complete(flowProcess : FlowProcess[_], context : Tuple) = context
  }
  class ExtremumBy(choosemax : Boolean, arguments : Fields, result : Fields) extends AggregateBy (
        arguments,
        new ExtremumFunctor(choosemax, result),
        new ExtremumAggregator(choosemax, result))
}
