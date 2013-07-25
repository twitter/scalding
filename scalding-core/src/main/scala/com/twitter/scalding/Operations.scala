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
import com.twitter.chill.MeatLocker
import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration

import com.esotericsoftware.kryo.Kryo;

import com.twitter.algebird.{Semigroup, SummingCache}
import com.twitter.scalding.mathematics.Poisson
import com.twitter.chill.KryoPool
import com.twitter.chill.config.{Config, ConfiguredInstantiator}
import com.twitter.chill.config.ConfiguredInstantiator

object CascadingUtils {
  def flowProcessToConfig(fp : FlowProcess[_]) : Config = new Config {
    def get(k: String) = Option(fp.getProperty(k)).map { _.toString }.getOrElse(null)
    // We only read when creating KryoPools, we should have made ReadableConfig
    // https://github.com/twitter/chill/issues/113
    def set(k: String, v: String) = sys.error("cannot set keys in cascading")
  }
  def kryoFor(fp : FlowProcess[_]) : KryoPool = {
    val conf = flowProcessToConfig(fp)
    /**
      * The KryoPool size of 10 is arbitrary; this Kryo instance is
      * only used to serialize Scalding's internal functions and
      * operations (not to serialize actual items in the flow), so 10
      * seemed as good a choice as any.
      */
    KryoPool.withByteArrayOutputStream(10, new ConfiguredInstantiator(conf))
  }
}

import CascadingUtils.kryoFor

  class FlatMapFunction[S,T](@transient fn : S => TraversableOnce[T], fields : Fields,
    conv : TupleConverter[S], set : TupleSetter[T])
    extends BaseOperation[Any](fields) with Function[Any] {
    val lockedFn = MeatLocker(fn)
    def operate(flowProcess : FlowProcess[_], functionCall : FunctionCall[Any]) {
      lockedFn.get(conv(functionCall.getArguments)).foreach { arg : T =>
        val this_tup = set(arg)
        functionCall.getOutputCollector.add(this_tup)
      }
    }
  }

  class MapFunction[S,T](@transient fn : S => T, fields : Fields,
    conv : TupleConverter[S], set : TupleSetter[T])
    extends BaseOperation[Any](fields) with Function[Any] {
    val lockedFn = MeatLocker(fn)
    def operate(flowProcess : FlowProcess[_], functionCall : FunctionCall[Any]) {
      val res = lockedFn.get(conv(functionCall.getArguments))
      functionCall.getOutputCollector.add(set(res))
    }
  }

  /** An implementation of map-side combining which is appropriate for associative and commutative functions
   * If a cacheSize is given, it is used, else we query
   * the config for cascading.aggregateby.threshold (standard cascading param for an equivalent case)
   * else we use a default value of 100,000
   *
   * This keeps a cache of keys up to the cache-size, summing values as keys collide
   * On eviction, or completion of this Operation, the key-value pairs are put into outputCollector.
   *
   * This NEVER spills to disk and generally never be a performance penalty. If you have
   * poor locality in the keys, you just don't get any benefit but little added cost.
   *
   * Note this means that you may still have repeated keys in the output even on a single mapper
   * since the key space may be so large that you can't fit all of them in the cache at the same
   * time.
   *
   * You can use this with the Fields-API by doing:
   * {{{
   *  val msr = new MapsideReduce(Semigroup.from(fn), 'key, 'value, None)
   *  // MUST map onto the same key,value space (may be multiple fields)
   *  val mapSideReduced = pipe.eachTo(('key, 'value) -> ('key, 'value)) { _ => msr }
   * }}}
   * That said, this is equivalent to AggregateBy, and the only value is that it is much simpler than AggregateBy.
   * AggregateBy assumes several parallel reductions are happening, and thus has many loops, and array lookups
   * to deal with that.  Since this does many fewer allocations, and has a smaller code-path it may be faster for
   * the typed-API.
   */
  class MapsideReduce[V](commutativeSemigroup: Semigroup[V], keyFields: Fields, valueFields: Fields,
    cacheSize: Option[Int])(implicit conv: TupleConverter[V], set: TupleSetter[V])
    extends BaseOperation[SummingCache[Tuple,V]](Fields.join(keyFields, valueFields))
    with Function[SummingCache[Tuple,V]] {

    val DEFAULT_CACHE_SIZE = 100000
    val SIZE_CONFIG_KEY = "cascading.aggregateby.threshold"

    def cacheSize(fp: FlowProcess[_]): Int =
      cacheSize.orElse {
        Option(fp.getStringProperty(SIZE_CONFIG_KEY))
          .filterNot { _.isEmpty }
          .map { _.toInt }
      }
      .getOrElse( DEFAULT_CACHE_SIZE )

    override def prepare(flowProcess: FlowProcess[_], operationCall: OperationCall[SummingCache[Tuple,V]]) {
      //Set up the context:
      implicit val sg: Semigroup[V] = commutativeSemigroup
      val cache = SummingCache[Tuple,V](cacheSize(flowProcess))
      operationCall.setContext(cache)
    }

    @inline
    private def add(evicted: Option[Map[Tuple,V]], functionCall: FunctionCall[SummingCache[Tuple,V]]) {
      // Use iterator and while for optimal performance (avoid closures/fn calls)
      if(evicted.isDefined) {
        val it = evicted.get.iterator
        val tecol = functionCall.getOutputCollector
        while(it.hasNext) {
          val (key, value) = it.next
          // Safe to mutate this key as it is evicted from the map
          key.addAll(set(value))
          tecol.add(key)
        }
      }
    }

    override def operate(flowProcess: FlowProcess[_], functionCall: FunctionCall[SummingCache[Tuple,V]]) {
      val cache = functionCall.getContext
      val keyValueTE = functionCall.getArguments
      // Have to keep a copy of the key tuple because cascading will modify it
      val key = keyValueTE.selectEntry(keyFields).getTupleCopy
      val value = conv(keyValueTE.selectEntry(valueFields))
      add(cache.put(Map(key -> value)), functionCall)
    }

    override def flush(flowProcess: FlowProcess[_], operationCall: OperationCall[SummingCache[Tuple,V]]) {
      // Docs say it is safe to do this cast:
      // http://docs.cascading.org/cascading/2.1/javadoc/cascading/operation/Operation.html#flush(cascading.flow.FlowProcess, cascading.operation.OperationCall)
      val functionCall = operationCall.asInstanceOf[FunctionCall[SummingCache[Tuple,V]]]
      val cache = functionCall.getContext
      add(cache.flush, functionCall)
    }

    override def cleanup(flowProcess: FlowProcess[_], operationCall: OperationCall[SummingCache[Tuple,V]]) {
      // The cache may be large, but super sure we drop any reference to it ASAP
      // probably overly defensive, but it's super cheap.
      operationCall.setContext(null)
    }
  }

  /*
   * BaseOperation with support for context
   */
  abstract class SideEffectBaseOperation[C] (
    @transient bf: => C,                // begin function returns a context
    @transient ef: C => Unit,          // end function to clean up context object
    fields: Fields
   ) extends BaseOperation[C](fields) {
    val lockedBf = MeatLocker(() => bf)
    val lockedEf = MeatLocker(ef)
    override def prepare(flowProcess: FlowProcess[_], operationCall: OperationCall[C]) {
      operationCall.setContext(lockedBf.get.apply)
    }

    override def cleanup(flowProcess: FlowProcess[_], operationCall: OperationCall[C]) {
      lockedEf.get(operationCall.getContext)
    }
   }

  /*
   * A map function that allows state object to be set up and tear down.
   */
  class SideEffectMapFunction[S, C, T] (
    bf: => C,                // begin function returns a context
    @transient fn: (C, S) => T,         // function that takes a context and a tuple and generate a new tuple
    ef: C => Unit,           // end function to clean up context object
    fields: Fields,
    conv: TupleConverter[S],
    set: TupleSetter[T]
  ) extends SideEffectBaseOperation[C](bf, ef, fields) with Function[C] {
    val lockedFn = MeatLocker(fn)

    override def operate(flowProcess: FlowProcess[_], functionCall: FunctionCall[C]) {
      val context = functionCall.getContext
      val s = conv(functionCall.getArguments)
      val res = lockedFn.get(context, s)
      functionCall.getOutputCollector.add(set(res))
    }
  }

  /*
   * A flatmap function that allows state object to be set up and tear down.
   */
  class SideEffectFlatMapFunction[S, C, T] (
    bf: => C,                  // begin function returns a context
    @transient fn: (C, S) => TraversableOnce[T], // function that takes a context and a tuple, returns TraversableOnce of T
    ef: C => Unit,             // end function to clean up context object
    fields: Fields,
    conv: TupleConverter[S],
    set: TupleSetter[T]
  ) extends SideEffectBaseOperation[C](bf, ef, fields) with Function[C] {
    val lockedFn = MeatLocker(fn)

    override def operate(flowProcess: FlowProcess[_], functionCall: FunctionCall[C]) {
      val context = functionCall.getContext
      val s = conv(functionCall.getArguments)
      lockedFn.get(context, s) foreach { t => functionCall.getOutputCollector.add(set(t)) }
    }
  }

  class FilterFunction[T](@transient fn : T => Boolean, conv : TupleConverter[T])
      extends BaseOperation[Any] with Filter[Any] {
    val lockedFn = MeatLocker(fn)

    def isRemove(flowProcess : FlowProcess[_], filterCall : FilterCall[Any]) = {
      !lockedFn.get(conv(filterCall.getArguments))
    }
  }

  // All the following are operations for use in GroupBuilder

  class FoldAggregator[T,X](@transient fn : (X,T) => X, init : X, fields : Fields,
    conv : TupleConverter[T], set : TupleSetter[X])
    extends BaseOperation[X](fields) with Aggregator[X] {
    val lockedFn = MeatLocker(fn)

    @transient var kryoPool: KryoPool = null
    private def copyInit(flowProcess : FlowProcess[_]) = {
      if(null == kryoPool) kryoPool = kryoFor(flowProcess)
      kryoPool.deepCopy(init)
    }

    def start(flowProcess : FlowProcess[_], call : AggregatorCall[X]) {
      val deepCopyInit = copyInit(flowProcess)
      call.setContext(deepCopyInit)
    }

    def aggregate(flowProcess : FlowProcess[_], call : AggregatorCall[X]) {
      val left = call.getContext
      val right = conv(call.getArguments)
      call.setContext(lockedFn.get(left, right))
    }

    def complete(flowProcess : FlowProcess[_], call : AggregatorCall[X]) {
      emit(flowProcess, call)
    }

    def emit(flowProcess : FlowProcess[_], call : AggregatorCall[X]) {
      call.getOutputCollector.add(set(call.getContext))
    }
  }

  /*
   * fields are the declared fields of this aggregator
   */
  class MRMAggregator[T,X,U](
    @transient inputFsmf : T => X,
    @transient inputRfn : (X,X) => X,
    @transient inputMrfn : X => U,
    fields : Fields, conv : TupleConverter[T], set : TupleSetter[U])
    extends BaseOperation[Tuple](fields) with Aggregator[Tuple] {
    val fsmf = MeatLocker(inputFsmf)
    val rfn = MeatLocker(inputRfn)
    val mrfn = MeatLocker(inputMrfn)

    // The context is a singleton Tuple, which is mutable so
    // we don't have to allocate at every step of the loop:
    def start(flowProcess : FlowProcess[_], call : AggregatorCall[Tuple]) {
        call.setContext(null)
    }

    def extractArgument(call : AggregatorCall[Tuple]) : X = fsmf.get(conv(call.getArguments))

    def aggregate(flowProcess : FlowProcess[_], call : AggregatorCall[Tuple]) {
      val arg = extractArgument(call)
      val ctx = call.getContext
      if (null == ctx) {
        // Initialize the context, this is the only allocation done by this loop.
        val newCtx = Tuple.size(1)
        newCtx.set(0, arg.asInstanceOf[AnyRef])
        call.setContext(newCtx)
      }
      else {
        // Mutate the context:
        val oldValue = ctx.getObject(0).asInstanceOf[X]
        val newValue = rfn.get(oldValue, arg)
        ctx.set(0, newValue.asInstanceOf[AnyRef])
      }
    }

    def complete(flowProcess : FlowProcess[_], call : AggregatorCall[Tuple]) {
      val ctx = call.getContext
      if (null != ctx) {
        val lastValue = ctx.getObject(0).asInstanceOf[X]
        // Make sure to drop the reference to the lastValue as soon as possible (it may be big)
        call.setContext(null)
        call.getOutputCollector.add(set(mrfn.get(lastValue)))
      }
      else {
        throw new Exception("MRMAggregator completed without any args")
      }
    }
  }

  /**
   * This handles the mapReduceMap work on the map-side of the operation.  The code below
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
        val res = context.getObject(0).asInstanceOf[X]
        // Make sure we remove the ref to the context ASAP:
        context.set(0, null)
        finish(res)
      }
    }
  }

  /**
   * This handles the mapReduceMap work on the map-side of the operation.  The code below
   * attempts to be optimal with respect to memory allocations and performance, not functional
   * style purity.
   */
  class MRMFunctor[T,X](
    @transient inputMrfn : T => X,
    @transient inputRfn : (X, X) => X,
    fields : Fields,
    conv : TupleConverter[T], set : TupleSetter[X])
    extends FoldFunctor[X](fields) {

    val mrfn = MeatLocker(inputMrfn)
    val rfn = MeatLocker(inputRfn)

    override def first(args : TupleEntry) : X = mrfn.get(conv(args))
    override def subsequent(oldValue : X, newArgs : TupleEntry) = {
      val right = mrfn.get(conv(newArgs))
      rfn.get(oldValue, right)
    }
    override def finish(lastValue : X) = set(lastValue)
  }

  /**
   * MapReduceMapBy Class
   */
  class MRMBy[T,X,U](arguments : Fields,
                   middleFields : Fields,
                   declaredFields : Fields,
                   mfn : T => X,
                   rfn : (X,X) => X,
                   mfn2 : X => U,
                   startConv : TupleConverter[T],
                   midSet : TupleSetter[X],
                   midConv : TupleConverter[X],
                   endSet : TupleSetter[U]) extends AggregateBy(
        arguments,
        new MRMFunctor[T,X](mfn, rfn, middleFields, startConv, midSet),
        new MRMAggregator[X,X,U](args => args, rfn, mfn2, declaredFields, midConv, endSet))

  class BufferOp[I,T,X](
    init : I,
    @transient inputIterfn : (I, Iterator[T]) => TraversableOnce[X],
    fields : Fields, conv : TupleConverter[T], set : TupleSetter[X])
    extends BaseOperation[Any](fields) with Buffer[Any] {
    val iterfn = MeatLocker(inputIterfn)

    @transient var kryoPool: KryoPool = null
    private def copyInit(flowProcess : FlowProcess[_]) = {
      if(null == kryoPool) kryoPool = kryoFor(flowProcess)
      kryoPool.deepCopy(init)
    }
    def operate(flowProcess : FlowProcess[_], call : BufferCall[Any]) {
      val deepCopyInit = copyInit(flowProcess)
      val oc = call.getOutputCollector
      val in = call.getArgumentsIterator.asScala.map { entry => conv(entry) }
      iterfn.get(deepCopyInit, in).foreach { x => oc.add(set(x)) }
    }
  }

  /*
   * A buffer that allows state object to be set up and tear down.
   */
  class SideEffectBufferOp[I,T,C,X](
    init : I,
    bf: => C,                  // begin function returns a context
    @transient inputIterfn: (I, C, Iterator[T]) => TraversableOnce[X],
    ef: C => Unit,             // end function to clean up context object
    fields: Fields,
    conv: TupleConverter[T],
    set: TupleSetter[X]
  ) extends SideEffectBaseOperation[C](bf, ef, fields) with Buffer[C] {
    val iterfn = MeatLocker(inputIterfn)

    @transient var kryoPool: KryoPool = null
    private def copyInit(flowProcess : FlowProcess[_]) = {
      if(null == kryoPool) kryoPool = kryoFor(flowProcess)
      kryoPool.deepCopy(init)
    }
    def operate(flowProcess : FlowProcess[_], call : BufferCall[C]) {
      val deepCopyInit = copyInit(flowProcess)
      val context = call.getContext
      val oc = call.getOutputCollector
      val in = call.getArgumentsIterator.asScala.map { entry => conv(entry) }
      iterfn.get(deepCopyInit, context, in).foreach { x => oc.add(set(x)) }
    }
  }

  class SampleWithReplacement(frac : Double, val seed : Int = new scala.util.Random().nextInt) extends BaseOperation[Poisson]() with Function[Poisson] {
    override def prepare(flowProcess : FlowProcess[_], operationCall : OperationCall[Poisson]) {
      super.prepare(flowProcess, operationCall)
      val p = new Poisson(frac, seed)
      operationCall.setContext( p );
    }

    def operate(flowProcess : FlowProcess[_], functionCall : FunctionCall[Poisson]) {
      val r = functionCall.getContext.nextInt
      for (i <- 0 until r)
        functionCall.getOutputCollector().add( Tuple.NULL )
    }
  }
}
