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
package com.twitter.scalding.typed

import java.io.{ OutputStream, InputStream, Serializable }
import java.util.{ Random, UUID }

import cascading.flow.FlowDef
import cascading.pipe.{ Each, Pipe }
import cascading.tap.Tap
import cascading.tuple.{ Fields, TupleEntry }
import com.twitter.algebird.{ Aggregator, Batched, Monoid, Semigroup }
import com.twitter.scalding.TupleConverter.{ TupleEntryConverter, singleConverter, tuple2Converter }
import com.twitter.scalding.TupleSetter.{ singleSetter, tup2Setter }
import com.twitter.scalding._
import com.twitter.scalding.serialization.OrderedSerialization
import com.twitter.scalding.serialization.OrderedSerialization.Result
import com.twitter.scalding.serialization.macros.impl.BinaryOrdering
import com.twitter.scalding.serialization.macros.impl.BinaryOrdering._

import scala.util.Try

/**
 * factory methods for TypedPipe, which is the typed representation of distributed lists in scalding.
 * This object is here rather than in the typed package because a lot of code was written using
 * the functions in the object, which we do not see how to hide with package object tricks.
 */
object TypedPipe extends Serializable {
  import Dsl.flowDefToRichFlowDef

  /**
   * Create a TypedPipe from a cascading Pipe, some Fields and the type T
   * Avoid this if you can. Prefer from(TypedSource).
   */
  def from[T](pipe: Pipe, fields: Fields)(implicit flowDef: FlowDef, mode: Mode, conv: TupleConverter[T]): TypedPipe[T] = {
    val localFlow = flowDef.onlyUpstreamFrom(pipe)
    new TypedPipeInst[T](pipe, fields, localFlow, mode, Converter(conv))
  }

  /**
   * Create a TypedPipe from a TypedSource. This is the preferred way to make a TypedPipe
   */
  def from[T](source: TypedSource[T]): TypedPipe[T] =
    TypedPipeFactory({ (fd, mode) =>
      val pipe = source.read(fd, mode)
      from(pipe, source.sourceFields)(fd, mode, source.converter)
    })

  /**
   * Create a TypedPipe from an Iterable in memory.
   */
  def from[T](iter: Iterable[T]): TypedPipe[T] =
    IterablePipe[T](iter)

  /**
   * Input must be a Pipe with exactly one Field
   * Avoid this method and prefer from(TypedSource) if possible
   */
  def fromSingleField[T](pipe: Pipe)(implicit fd: FlowDef, mode: Mode): TypedPipe[T] =
    from(pipe, new Fields(0))(fd, mode, singleConverter[T])

  /**
   * Create an empty TypedPipe. This is sometimes useful when a method must return
   * a TypedPipe, but sometimes at runtime we can check a condition and see that
   * it should be empty.
   * This is the zero of the Monoid[TypedPipe]
   */
  def empty: TypedPipe[Nothing] = EmptyTypedPipe

  /**
   * This enables pipe.hashJoin(that) or pipe.join(that) syntax
   * This is a safe enrichment because hashJoinable and CoGroupable are
   * only used in the argument position or to give cogroup, join, leftJoin, rightJoin, outerJoin
   * methods. Since those methods are unlikely to be used on TypedPipe in the future, this
   * enrichment seems safe.
   *
   * This method is the Vitaly-was-right method.
   */
  implicit def toHashJoinable[K, V](pipe: TypedPipe[(K, V)])(implicit ord: Ordering[K]): HashJoinable[K, V] =
    new HashJoinable[K, V] {
      def mapped = pipe
      def keyOrdering = ord
      def reducers = None
      val descriptions: Seq[String] = LineNumber.tryNonScaldingCaller.map(_.toString).toList
      def joinFunction = CoGroupable.castingJoinFunction[V]
    }

  /**
   * TypedPipe instances are monoids. They are isomorphic to multisets.
   */
  implicit def typedPipeMonoid[T]: Monoid[TypedPipe[T]] = new Monoid[TypedPipe[T]] {
    def zero = TypedPipe.empty
    def plus(left: TypedPipe[T], right: TypedPipe[T]): TypedPipe[T] =
      left ++ right
  }

  private val identityOrdering: OrderedSerialization[Int] = {
    val delegate = BinaryOrdering.ordSer[Int]
    new OrderedSerialization[Int] {
      override def compareBinary(a: InputStream, b: InputStream): Result = delegate.compareBinary(a, b)
      override def compare(x: Int, y: Int): Int = delegate.compare(x, y)
      override def dynamicSize(t: Int): Option[Int] = delegate.dynamicSize(t)
      override def write(out: OutputStream, t: Int): Try[Unit] = delegate.write(out, t)
      override def read(in: InputStream): Try[Int] = delegate.read(in)
      override def staticSize: Option[Int] = delegate.staticSize
      override def hash(x: Int): Int = x
    }
  }
}

/**
 * Think of a TypedPipe as a distributed unordered list that may or may not yet
 * have been materialized in memory or disk.
 *
 * Represents a phase in a distributed computation on an input data source
 * Wraps a cascading Pipe object, and holds the transformation done up until that point
 */
trait TypedPipe[+T] extends Serializable {

  /**
   * Implements a cross product.  The right side should be tiny
   * This gives the same results as
   * {code for { l <- list1; l2 <- list2 } yield (l, l2) }
   */
  def cross[U](tiny: TypedPipe[U]): TypedPipe[(T, U)]

  /**
   * This is the fundamental mapper operation.
   * It behaves in a way similar to List.flatMap, which means that each
   * item is fed to the input function, which can return 0, 1, or many outputs
   * (as a TraversableOnce) per input. The returned results will be iterated through once
   * and then flattened into a single TypedPipe which is passed to the next step in the
   * pipeline.
   *
   * This behavior makes it a powerful operator -- it can be used to filter records
   * (by returning 0 items for a given input), it can be used the way map is used
   * (by returning 1 item per input), it can be used to explode 1 input into many outputs,
   * or even a combination of all of the above at once.
   */
  def flatMap[U](f: T => TraversableOnce[U]): TypedPipe[U]

  /**
   * Export back to a raw cascading Pipe. useful for interop with the scalding
   * Fields API or with Cascading code.
   * Avoid this if possible. Prefer to write to TypedSink.
   */
  final def toPipe[U >: T](fieldNames: Fields)(implicit flowDef: FlowDef, mode: Mode, setter: TupleSetter[U]): Pipe = {
    import Dsl._
    // Ensure we hook into all pipes coming out of the typed API to apply the FlowState's properties on their pipes
    val pipe = asPipe[U](fieldNames).applyFlowConfigProperties(flowDef)
    RichPipe.setPipeDescriptionFrom(pipe, LineNumber.tryNonScaldingCaller)
  }

  /**
   * Provide the internal implementation to get from a typed pipe to a cascading Pipe
   */
  private[typed] def asPipe[U >: T](fieldNames: Fields)(implicit flowDef: FlowDef, mode: Mode, setter: TupleSetter[U]): Pipe

  /////////////////////////////////////////////
  //
  // The following have default implementations in terms of the above
  //
  /////////////////////////////////////////////

  import Dsl._

  /**
   * Merge two TypedPipes (no order is guaranteed)
   * This is only realized when a group (or join) is
   * performed.
   */
  def ++[U >: T](other: TypedPipe[U]): TypedPipe[U] = other match {
    case EmptyTypedPipe => this
    case IterablePipe(thatIter) if thatIter.isEmpty => this
    case _ => MergedTypedPipe(this, other)
  }

  /**
   * Aggregate all items in this pipe into a single ValuePipe
   *
   * Aggregators are composable reductions that allow you to glue together
   * several reductions and process them in one pass.
   *
   * Same as groupAll.aggregate.values
   */
  def aggregate[B, C](agg: Aggregator[T, B, C]): ValuePipe[C] =
    ComputedValue(groupAll.aggregate(agg).values)

  /**
   * Put the items in this into the keys, and unit as the value in a Group
   * in some sense, this is the dual of groupAll
   */
  @annotation.implicitNotFound(msg = "For asKeys method to work, the type in TypedPipe must have an Ordering.")
  def asKeys[U >: T](implicit ord: Ordering[U]): Grouped[U, Unit] =
    map((_, ())).group

  /**
   * If T <:< U, then this is safe to treat as TypedPipe[U] due to covariance
   */
  protected def raiseTo[U](implicit ev: T <:< U): TypedPipe[U] =
    this.asInstanceOf[TypedPipe[U]]

  /**
   * Filter and map. See scala.collection.List.collect.
   * {@code
   *   collect { case Some(x) => fn(x) }
   * }
   */
  def collect[U](fn: PartialFunction[T, U]): TypedPipe[U] =
    filter(fn.isDefinedAt(_)).map(fn)

  /**
   * Attach a ValuePipe to each element this TypedPipe
   */
  def cross[V](p: ValuePipe[V]): TypedPipe[(T, V)] =
    p match {
      case EmptyValue => EmptyTypedPipe
      case LiteralValue(v) => map { (_, v) }
      case ComputedValue(pipe) => cross(pipe)
    }

  /** prints the current pipe to stdout */
  def debug: TypedPipe[T] = onRawSingle(_.debug)

  /** adds a description to the pipe */
  def withDescription(description: String): TypedPipe[T] = new WithDescriptionTypedPipe[T](this, description)

  /**
   * Returns the set of distinct elements in the TypedPipe
   * This is the same as: .map((_, ())).group.sum.keys
   * If you want a distinct while joining, consider:
   * instead of:
   * {@code
   *   a.join(b.distinct.asKeys)
   * }
   * manually do the distinct:
   * {@code
   *   a.join(b.asKeys.sum)
   * }
   * The latter creates 1 map/reduce phase rather than 2
   */
  @annotation.implicitNotFound(msg = "For distinct method to work, the type in TypedPipe must have an Ordering.")
  def distinct(implicit ord: Ordering[_ >: T]): TypedPipe[T] =
    asKeys(ord.asInstanceOf[Ordering[T]]).sum.keys

  /**
   * Returns the set of distinct elements identified by a given lambda extractor in the TypedPipe
   */
  @annotation.implicitNotFound(msg = "For distinctBy method to work, the type to distinct on in the TypedPipe must have an Ordering.")
  def distinctBy[U](fn: T => U, numReducers: Option[Int] = None)(implicit ord: Ordering[_ >: U]): TypedPipe[T] = {
    // cast because Ordering is not contravariant, but should be (and this cast is safe)
    implicit val ordT: Ordering[U] = ord.asInstanceOf[Ordering[U]]

    // Semigroup to handle duplicates for a given key might have different values.
    implicit val sg: Semigroup[T] = new Semigroup[T] {
      def plus(a: T, b: T) = b
    }

    val op = map { tup => (fn(tup), tup) }.sumByKey
    val reduced = numReducers match {
      case Some(red) => op.withReducers(red)
      case None => op
    }
    reduced.map(_._2)
  }

  /** Merge two TypedPipes of different types by using Either */
  def either[R](that: TypedPipe[R]): TypedPipe[Either[T, R]] =
    map(Left(_)) ++ (that.map(Right(_)))

  /**
   * Sometimes useful for implementing custom joins with groupBy + mapValueStream when you know
   * that the value/key can fit in memory. Beware.
   */
  def eitherValues[K, V, R](that: TypedPipe[(K, R)])(implicit ev: T <:< (K, V)): TypedPipe[(K, Either[V, R])] =
    mapValues { (v: V) => Left(v) } ++ (that.mapValues { (r: R) => Right(r) })

  /**
   * If you are going to create two branches or forks,
   * it may be more efficient to call this method first
   * which will create a node in the cascading graph.
   * Without this, both full branches of the fork will be
   * put into separate cascading pipes, which can, in some cases,
   * be slower.
   *
   * Ideally the planner would see this
   */
  def fork: TypedPipe[T] = onRawSingle(identity)

  /**
   * limit the output to at most count items, if at least count items exist.
   */
  def limit(count: Int): TypedPipe[T] =
    groupAll.bufferedTake(count).values

  /** Transform each element via the function f */
  def map[U](f: T => U): TypedPipe[U] = flatMap { t => Iterator(f(t)) }

  /** Transform only the values (sometimes requires giving the types due to scala type inference) */
  def mapValues[K, V, U](f: V => U)(implicit ev: T <:< (K, V)): TypedPipe[(K, U)] =
    raiseTo[(K, V)].map { case (k, v) => (k, f(v)) }

  /** Similar to mapValues, but allows to return a collection of outputs for each input value */
  def flatMapValues[K, V, U](f: V => TraversableOnce[U])(implicit ev: T <:< (K, V)): TypedPipe[(K, U)] =
    raiseTo[(K, V)].flatMap { case (k, v) => f(v).map { v2 => k -> v2 } }

  /**
   * Keep only items that satisfy this predicate
   */
  def filter(f: T => Boolean): TypedPipe[T] =
    flatMap { t => if (f(t)) Iterator(t) else Iterator.empty }

  // This is just to appease for comprehension
  def withFilter(f: T => Boolean): TypedPipe[T] = filter(f)

  /**
   * If T is a (K, V) for some V, then we can use this function to filter.
   * Prefer to use this if your filter only touches the key.
   *
   * This is here to match the function in KeyedListLike, where it is optimized
   */
  def filterKeys[K](fn: K => Boolean)(implicit ev: T <:< (K, Any)): TypedPipe[T] =
    filter { ka => fn(ka.asInstanceOf[(K, Any)]._1) }

  /**
   * Keep only items that don't satisfy the predicate.
   * `filterNot` is the same as `filter` with a negated predicate.
   */
  def filterNot(f: T => Boolean): TypedPipe[T] =
    filter(!f(_))

  /** flatten an Iterable */
  def flatten[U](implicit ev: T <:< TraversableOnce[U]): TypedPipe[U] =
    flatMap { _.asInstanceOf[TraversableOnce[U]] } // don't use ev which may not be serializable

  /**
   * flatten just the values
   * This is more useful on KeyedListLike, but added here to reduce assymmetry in the APIs
   */
  def flattenValues[K, U](implicit ev: T <:< (K, TraversableOnce[U])): TypedPipe[(K, U)] =
    raiseTo[(K, TraversableOnce[U])].flatMap { case (k, us) => us.map((k, _)) }

  protected def onRawSingle(onPipe: Pipe => Pipe): TypedPipe[T] = {
    val self = this
    TypedPipeFactory({ (fd, m) =>
      val pipe = self.toPipe[T](new Fields(java.lang.Integer.valueOf(0)))(fd, m, singleSetter)
      TypedPipe.fromSingleField[T](onPipe(pipe))(fd, m)
    })
  }

  /**
   * Force a materialization of this pipe prior to the next operation.
   * This is useful if you filter almost everything before a hashJoin, for instance.
   * This is useful for experts who see some heuristic of the planner causing
   * slower performance.
   */
  def forceToDisk: TypedPipe[T] = onRawSingle(_.forceToDisk)

  /**
   * This is the default means of grouping all pairs with the same key. Generally this triggers 1 Map/Reduce transition
   */
  def group[K, V](implicit ev: <:<[T, (K, V)], ord: Ordering[K]): Grouped[K, V] =
    //If the type of T is not (K,V), then at compile time, this will fail.  It uses implicits to do
    //a compile time check that one type is equivalent to another.  If T is not (K,V), we can't
    //automatically group.  We cast because it is safe to do so, and we need to convert to K,V, but
    //the ev is not needed for the cast.  In fact, you can do the cast with ev(t) and it will return
    //it as (K,V), but the problem is, ev is not serializable.  So we do the cast, which due to ev
    //being present, will always pass.
    Grouped(raiseTo[(K, V)]).withDescription(LineNumber.tryNonScaldingCaller.map(_.toString))

  /** Send all items to a single reducer */
  def groupAll: Grouped[Unit, T] = groupBy(x => ())(ordSer[Unit]).withReducers(1)

  /** Given a key function, add the key, then call .group */
  def groupBy[K](g: T => K)(implicit ord: Ordering[K]): Grouped[K, T] =
    map { t => (g(t), t) }.group

  /** Group using an explicit Ordering on the key. */
  def groupWith[K, V](ord: Ordering[K])(implicit ev: <:<[T, (K, V)]): Grouped[K, V] = group(ev, ord)

  /**
   * Forces a shuffle by randomly assigning each item into one
   * of the partitions.
   *
   * This is for the case where you mappers take a long time, and
   * it is faster to shuffle them to more reducers and then operate.
   *
   * You probably want shard if you are just forcing a shuffle.
   */
  def groupRandomly(partitions: Int): Grouped[Int, T] = {
    // Make it lazy so all mappers get their own:
    lazy val rng = new java.util.Random(123) // seed this so it is repeatable
    groupBy { _ => rng.nextInt(partitions) }(TypedPipe.identityOrdering)
      .withReducers(partitions)
  }

  /**
   * Partitions this into two pipes according to a predicate.
   *
   * Sometimes what you really want is a groupBy in these cases.
   */
  def partition(p: T => Boolean): (TypedPipe[T], TypedPipe[T]) = {
    val forked = fork
    (forked.filter(p), forked.filterNot(p))
  }

  private[this] def defaultSeed: Long = System.identityHashCode(this) * 2654435761L ^ System.currentTimeMillis
  /**
   * Sample a fraction (between 0 and 1) uniformly independently at random each element of the pipe
   * does not require a reduce step.
   */
  def sample(fraction: Double): TypedPipe[T] = sample(fraction, defaultSeed)
  /**
   * Sample a fraction (between 0 and 1) uniformly independently at random each element of the pipe with
   * a given seed.
   * Does not require a reduce step.
   */
  def sample(fraction: Double, seed: Long): TypedPipe[T] = {
    require(0.0 <= fraction && fraction <= 1.0, s"got $fraction which is an invalid fraction")

    // Make sure to fix the seed, otherwise restarts cause subtle errors
    lazy val rand = new Random(seed)
    filter(_ => rand.nextDouble < fraction)
  }

  /**
   * This does a sum of values WITHOUT triggering a shuffle.
   * the contract is, if followed by a group.sum the result is the same
   * with or without this present, and it never increases the number of
   * items. BUT due to the cost of caching, it might not be faster if
   * there is poor key locality.
   *
   * It is only useful for expert tuning,
   * and best avoided unless you are struggling with performance problems.
   * If you are not sure you need this, you probably don't.
   *
   * The main use case is to reduce the values down before a key expansion
   * such as is often done in a data cube.
   */
  def sumByLocalKeys[K, V](implicit ev: T <:< (K, V), sg: Semigroup[V]): TypedPipe[(K, V)] = {
    val fields: Fields = ('key, 'value)
    val selfKV = raiseTo[(K, V)]
    TypedPipeFactory({ (fd, mode) =>
      val pipe = selfKV.toPipe(fields)(fd, mode, tup2Setter)
      val msr = new MapsideReduce(sg, 'key, 'value, None)(singleConverter[V], singleSetter[V])
      TypedPipe.from[(K, V)](pipe.eachTo(fields -> fields) { _ => msr }, fields)(fd, mode, tuple2Converter)
    })
  }

  /**
   * Used to force a shuffle into a given size of nodes.
   * Only use this if your mappers are taking far longer than
   * the time to shuffle.
   */
  def shard(partitions: Int): TypedPipe[T] = groupRandomly(partitions).forceToReducers.values

  /**
   * Reasonably common shortcut for cases of total associative/commutative reduction
   * returns a ValuePipe with only one element if there is any input, otherwise EmptyValue.
   */
  def sum[U >: T](implicit plus: Semigroup[U]): ValuePipe[U] = {
    // every 1000 items, compact.
    lazy implicit val batchedSG: Semigroup[Batched[U]] = Batched.compactingSemigroup[U](1000)
    ComputedValue(map { t => ((), Batched[U](t)) }
      .sumByLocalKeys
      // remove the Batched before going to the reducers
      .map { case (_, batched) => batched.sum }
      .groupAll
      .forceToReducers
      .sum
      .values)
  }

  /**
   * Reasonably common shortcut for cases of associative/commutative reduction by Key
   */
  def sumByKey[K, V](implicit ev: T <:< (K, V), ord: Ordering[K], plus: Semigroup[V]): UnsortedGrouped[K, V] =
    group[K, V].sum[V]

  /**
   * This is used when you are working with Execution[T] to create loops.
   * You might do this to checkpoint and then flatMap Execution to continue
   * from there. Probably only useful if you need to flatMap it twice to fan
   * out the data into two children jobs.
   *
   * This writes the current TypedPipe into a temporary file
   * and then opens it after complete so that you can continue from that point
   */
  def forceToDiskExecution: Execution[TypedPipe[T]] = {
    val cachedRandomUUID = java.util.UUID.randomUUID
    lazy val inMemoryDest = new MemorySink[T]

    def temporaryPath(conf: Config, uuid: UUID): String = {
      val tmpDir = conf.get("hadoop.tmp.dir")
        .orElse(conf.get("cascading.tmp.dir"))
        .getOrElse("/tmp")

      tmpDir + "/scalding/snapshot-" + uuid + ".seq"
    }

    def hadoopTypedSource(conf: Config): TypedSource[T] with TypedSink[T] = {
      // come up with unique temporary filename, use the config here
      // TODO: refactor into TemporarySequenceFile class
      val tmpSeq = temporaryPath(conf, cachedRandomUUID)
      source.TypedSequenceFile[T](tmpSeq)

    }
    val writeFn = { (conf: Config, mode: Mode) =>
      mode match {
        case _: CascadingLocal => // Local or Test mode
          (this, inMemoryDest)
        case _: HadoopMode =>
          (this, hadoopTypedSource(conf))
      }
    }

    val readFn = { (conf: Config, mode: Mode) =>
      mode match {
        case _: CascadingLocal => // Local or Test mode
          TypedPipe.from(inMemoryDest.readResults)
        case _: HadoopMode =>
          TypedPipe.from(hadoopTypedSource(conf))
      }
    }

    val filesToDeleteFn = { (conf: Config, mode: Mode) =>
      mode match {
        case _: CascadingLocal => // Local or Test mode
          Set[String]()
        case _: HadoopMode =>
          Set(temporaryPath(conf, cachedRandomUUID))
      }
    }

    Execution.write(writeFn, readFn, filesToDeleteFn)
  }

  /**
   * This gives an Execution that when run evaluates the TypedPipe,
   * writes it to disk, and then gives you an Iterable that reads from
   * disk on the submit node each time .iterator is called.
   * Because of how scala Iterables work, mapping/flatMapping/filtering
   * the Iterable forces a read of the entire thing. If you need it to
   * be lazy, call .iterator and use the Iterator inside instead.
   */
  def toIterableExecution: Execution[Iterable[T]] =
    forceToDiskExecution.flatMap(_.toIterableExecution)

  /** use a TupleUnpacker to flatten U out into a cascading Tuple */
  def unpackToPipe[U >: T](fieldNames: Fields)(implicit fd: FlowDef, mode: Mode, up: TupleUnpacker[U]): Pipe = {
    val setter = up.newSetter(fieldNames)
    toPipe[U](fieldNames)(fd, mode, setter)
  }

  /**
   * This attaches a function that is called at the end of the map phase on
   * EACH of the tasks that are executing.
   * This is for expert use only. You probably won't ever need it. Try hard
   * to avoid it. Execution also has onComplete that can run when an Execution
   * has completed.
   */
  def onComplete(fn: () => Unit): TypedPipe[T] = new WithOnComplete[T](this, fn)

  /**
   * Safely write to a TypedSink[T]. If you want to write to a Source (not a Sink)
   * you need to do something like: toPipe(fieldNames).write(dest)
   * @return a pipe equivalent to the current pipe.
   */
  def write(dest: TypedSink[T])(implicit flowDef: FlowDef, mode: Mode): TypedPipe[T] = {
    // Make sure that we don't render the whole pipeline twice:
    val res = fork
    dest.writeFrom(res.toPipe[T](dest.sinkFields)(flowDef, mode, dest.setter))
    res
  }

  /**
   * This is the functionally pure approach to building jobs. Note,
   * that you have to call run on the result or flatMap/zip it
   * into an Execution that is run for anything to happen here.
   */
  def writeExecution(dest: TypedSink[T]): Execution[Unit] =
    Execution.write(this, dest)

  /**
   * If you want to write to a specific location, and then read from
   * that location going forward, use this.
   */
  def writeThrough[U >: T](dest: TypedSink[T] with TypedSource[U]): Execution[TypedPipe[U]] =
    Execution.write(this, dest, TypedPipe.from(dest))

  /**
   * If you want to writeThrough to a specific file if it doesn't already exist,
   * and otherwise just read from it going forward, use this.
   */
  def make[U >: T](dest: Source with TypedSink[T] with TypedSource[U]): Execution[TypedPipe[U]] =
    Execution.getMode.flatMap { mode =>
      try {
        dest.validateTaps(mode)
        Execution.from(TypedPipe.from(dest))
      } catch {
        case ivs: InvalidSourceException => writeThrough(dest)
      }
    }

  /** Just keep the keys, or ._1 (if this type is a Tuple2) */
  def keys[K](implicit ev: <:<[T, (K, Any)]): TypedPipe[K] =
    // avoid capturing ev in the closure:
    raiseTo[(K, Any)].map(_._1)

  /** swap the keys with the values */
  def swap[K, V](implicit ev: <:<[T, (K, V)]): TypedPipe[(V, K)] =
    raiseTo[(K, V)].map(_.swap)

  /** Just keep the values, or ._2 (if this type is a Tuple2) */
  def values[V](implicit ev: <:<[T, (Any, V)]): TypedPipe[V] =
    raiseTo[(Any, V)].map(_._2)

  /**
   * ValuePipe may be empty, so, this attaches it as an Option
   * cross is the same as leftCross(p).collect { case (t, Some(v)) => (t, v) }
   */
  def leftCross[V](p: ValuePipe[V]): TypedPipe[(T, Option[V])] =
    p match {
      case EmptyValue => map { (_, None) }
      case LiteralValue(v) => map { (_, Some(v)) }
      case ComputedValue(pipe) => leftCross(pipe)
    }

  /** uses hashJoin but attaches None if thatPipe is empty */
  def leftCross[V](thatPipe: TypedPipe[V]): TypedPipe[(T, Option[V])] =
    map(((), _)).hashLeftJoin(thatPipe.groupAll).values

  /**
   * common pattern of attaching a value and then map
   * recommended style:
   * {@code
   *  mapWithValue(vpu) {
   *    case (t, Some(u)) => op(t, u)
   *    case (t, None) => // if you never expect this:
   *      sys.error("unexpected empty value pipe")
   *  }
   * }
   */
  def mapWithValue[U, V](value: ValuePipe[U])(f: (T, Option[U]) => V): TypedPipe[V] =
    leftCross(value).map(t => f(t._1, t._2))

  /**
   * common pattern of attaching a value and then flatMap
   * recommended style:
   * {@code
   *  flatMapWithValue(vpu) {
   *    case (t, Some(u)) => op(t, u)
   *    case (t, None) => // if you never expect this:
   *      sys.error("unexpected empty value pipe")
   *  }
   * }
   */
  def flatMapWithValue[U, V](value: ValuePipe[U])(f: (T, Option[U]) => TraversableOnce[V]): TypedPipe[V] =
    leftCross(value).flatMap(t => f(t._1, t._2))

  /**
   * common pattern of attaching a value and then filter
   * recommended style:
   * {@code
   *  filterWithValue(vpu) {
   *    case (t, Some(u)) => op(t, u)
   *    case (t, None) => // if you never expect this:
   *      sys.error("unexpected empty value pipe")
   *  }
   * }
   */
  def filterWithValue[U](value: ValuePipe[U])(f: (T, Option[U]) => Boolean): TypedPipe[T] =
    leftCross(value).filter(t => f(t._1, t._2)).map(_._1)

  /**
   * These operations look like joins, but they do not force any communication
   * of the current TypedPipe. They are mapping operations where this pipe is streamed
   * through one item at a time.
   *
   * WARNING These behave semantically very differently than cogroup.
   * This is because we handle (K,V) tuples on the left as we see them.
   * The iterable on the right is over all elements with a matching key K, and it may be empty
   * if there are no values for this key K.
   */
  def hashCogroup[K, V, W, R](smaller: HashJoinable[K, W])(joiner: (K, V, Iterable[W]) => Iterator[R])(implicit ev: TypedPipe[T] <:< TypedPipe[(K, V)]): TypedPipe[(K, R)] =
    smaller.hashCogroupOn(ev(this))(joiner)

  /** Do an inner-join without shuffling this TypedPipe, but replicating argument to all tasks */
  def hashJoin[K, V, W](smaller: HashJoinable[K, W])(implicit ev: TypedPipe[T] <:< TypedPipe[(K, V)]): TypedPipe[(K, (V, W))] =
    hashCogroup[K, V, W, (V, W)](smaller)(Joiner.hashInner2)

  /** Do an leftjoin without shuffling this TypedPipe, but replicating argument to all tasks */
  def hashLeftJoin[K, V, W](smaller: HashJoinable[K, W])(implicit ev: TypedPipe[T] <:< TypedPipe[(K, V)]): TypedPipe[(K, (V, Option[W]))] =
    hashCogroup[K, V, W, (V, Option[W])](smaller)(Joiner.hashLeft2)

  /**
   * For each element, do a map-side (hash) left join to look up a value
   */
  def hashLookup[K >: T, V](grouped: HashJoinable[K, V]): TypedPipe[(K, Option[V])] =
    map((_, ()))
      .hashLeftJoin(grouped)
      .map { case (t, (_, optV)) => (t, optV) }

  /**
   * Enables joining when this TypedPipe has some keys with many many values and
   * but many with very few values. For instance, a graph where some nodes have
   * millions of neighbors, but most have only a few.
   *
   * We build a (count-min) sketch of each key's frequency, and we use that
   * to shard the heavy keys across many reducers.
   * This increases communication cost in order to reduce the maximum time needed
   * to complete the join.
   *
   * {@code pipe.sketch(100).join(thatPipe) }
   * will add an extra map/reduce job over a standard join to create the count-min-sketch.
   * This will generally only be beneficial if you have really heavy skew, where without
   * this you have 1 or 2 reducers taking hours longer than the rest.
   */
  def sketch[K, V](reducers: Int,
    eps: Double = 1.0E-5, //272k width = 1MB per row
    delta: Double = 0.01, //5 rows (= 5 hashes)
    seed: Int = 12345)(implicit ev: TypedPipe[T] <:< TypedPipe[(K, V)],
      serialization: K => Array[Byte],
      ordering: Ordering[K]): Sketched[K, V] =
    Sketched(ev(this), reducers, delta, eps, seed)

  /**
   * If any errors happen below this line, but before a groupBy, write to a TypedSink
   */
  def addTrap[U >: T](trapSink: Source with TypedSink[T])(implicit conv: TupleConverter[U]): TypedPipe[U] =
    TypedPipeFactory({ (flowDef, mode) =>
      val fields = trapSink.sinkFields
      // TODO: with diamonds in the graph, this might not be correct
      val pipe = RichPipe.assignName(fork.toPipe[T](fields)(flowDef, mode, trapSink.setter))
      flowDef.addTrap(pipe, trapSink.createTap(Write)(mode))
      TypedPipe.from[U](pipe, fields)(flowDef, mode, conv)
    })
}

/**
 * This object is the EmptyTypedPipe. Prefer to create it with TypedPipe.empty
 */
final case object EmptyTypedPipe extends TypedPipe[Nothing] {

  override def aggregate[B, C](agg: Aggregator[Nothing, B, C]): ValuePipe[C] = EmptyValue

  // Cross product with empty is always empty.
  override def cross[U](tiny: TypedPipe[U]): TypedPipe[(Nothing, U)] = this

  override def distinct(implicit ord: Ordering[_ >: Nothing]) = this

  override def flatMap[U](f: Nothing => TraversableOnce[U]) = this

  override def fork: TypedPipe[Nothing] = this

  override def forceToDisk = this

  override def leftCross[V](p: ValuePipe[V]) = this

  override def limit(count: Int) = this

  override def debug: TypedPipe[Nothing] = this

  override def ++[U >: Nothing](other: TypedPipe[U]): TypedPipe[U] = other

  override def asPipe[U >: Nothing](fieldNames: Fields)(implicit fd: FlowDef, mode: Mode, setter: TupleSetter[U]): Pipe =
    IterableSource(Iterable.empty, fieldNames)(setter, singleConverter[U]).read(fd, mode)

  override def toIterableExecution: Execution[Iterable[Nothing]] = Execution.from(Iterable.empty)

  override def forceToDiskExecution: Execution[TypedPipe[Nothing]] = Execution.from(this)

  override def sum[U >: Nothing](implicit plus: Semigroup[U]): ValuePipe[U] = EmptyValue

  override def sumByLocalKeys[K, V](implicit ev: Nothing <:< (K, V), sg: Semigroup[V]) = this

  override def hashCogroup[K, V, W, R](smaller: HashJoinable[K, W])(joiner: (K, V, Iterable[W]) => Iterator[R])(implicit ev: TypedPipe[Nothing] <:< TypedPipe[(K, V)]): TypedPipe[(K, R)] =
    this
}

/**
 * Creates a TypedPipe from an Iterable[T]. Prefer TypedPipe.from.
 *
 * If you avoid toPipe, this class is more efficient than IterableSource.
 */
final case class IterablePipe[T](iterable: Iterable[T]) extends TypedPipe[T] {

  override def aggregate[B, C](agg: Aggregator[T, B, C]): ValuePipe[C] =
    Some(iterable)
      .filterNot(_.isEmpty)
      .map(it => LiteralValue(agg(it)))
      .getOrElse(EmptyValue)

  override def ++[U >: T](other: TypedPipe[U]): TypedPipe[U] = other match {
    case IterablePipe(thatIter) => IterablePipe(iterable ++ thatIter)
    case EmptyTypedPipe => this
    case _ if iterable.isEmpty => other
    case _ => MergedTypedPipe(this, other)
  }

  override def cross[U](tiny: TypedPipe[U]) =
    tiny.flatMap { u => iterable.map { (_, u) } }

  override def filter(f: T => Boolean): TypedPipe[T] =
    iterable.filter(f) match {
      case eit if eit.isEmpty => EmptyTypedPipe
      case filtered => IterablePipe(filtered)
    }

  /**
   * When flatMap is called on an IterablePipe, we defer to make sure that f is
   * applied lazily, which avoids OOM issues when the returned value from the
   * map is larger than the input
   */
  override def flatMap[U](f: T => TraversableOnce[U]) =
    toSourcePipe.flatMap(f)

  override def fork: TypedPipe[T] = this

  override def forceToDisk = this

  override def limit(count: Int): TypedPipe[T] = IterablePipe(iterable.take(count))

  /**
   * When map is called on an IterablePipe, we defer to make sure that f is
   * applied lazily, which avoids OOM issues when the returned value from the
   * map is larger than the input
   */
  override def map[U](f: T => U): TypedPipe[U] =
    toSourcePipe.map(f)

  override def forceToDiskExecution: Execution[TypedPipe[T]] = Execution.from(this)

  override def sum[U >: T](implicit plus: Semigroup[U]): ValuePipe[U] =
    Semigroup.sumOption[U](iterable).map(LiteralValue(_))
      .getOrElse(EmptyValue)

  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  override def sumByLocalKeys[K, V](implicit ev: T <:< (K, V), sg: Semigroup[V]) = {
    val kvit = raiseTo[(K, V)] match {
      case IterablePipe(kviter) => kviter
      case p => sys.error("This must be IterablePipe: " + p.toString)
    }
    IterablePipe(kvit.groupBy(_._1)
      // use map to force this so it is not lazy.
      .map {
        case (k, kvs) =>
          // These lists are never empty, get is safe.
          (k, Semigroup.sumOption(kvs.iterator.map(_._2)).get)
      })
  }

  override def asPipe[U >: T](fieldNames: Fields)(implicit flowDef: FlowDef, mode: Mode, setter: TupleSetter[U]): Pipe =
    // It is slightly more efficient to use this rather than toSourcePipe.toPipe(fieldNames)
    IterableSource[U](iterable, fieldNames)(setter, singleConverter[U]).read(flowDef, mode)

  private[this] def toSourcePipe =
    TypedPipe.from(
      IterableSource[T](iterable, new Fields("0"))(singleSetter, singleConverter))

  override def toIterableExecution: Execution[Iterable[T]] = Execution.from(iterable)
}

/**
 * This is an implementation detail (and should be marked private)
 */
object TypedPipeFactory {
  def apply[T](next: (FlowDef, Mode) => TypedPipe[T]): TypedPipeFactory[T] = {
    val memo = new java.util.WeakHashMap[FlowDef, (Mode, TypedPipe[T])]()
    val fn = { (fd: FlowDef, m: Mode) =>
      memo.synchronized {
        memo.get(fd) match {
          case null =>
            val res = next(fd, m)
            memo.put(fd, (m, res))
            res
          case (memoMode, pipe) if memoMode == m => pipe
          case (memoMode, pipe) =>
            sys.error("FlowDef reused on different Mode. Original: %s, now: %s".format(memoMode, m))
        }
      }
    }
    new TypedPipeFactory(NoStackAndThen(fn.tupled))
  }
  def unapply[T](tp: TypedPipe[T]): Option[NoStackAndThen[(FlowDef, Mode), TypedPipe[T]]] =
    tp match {
      case tp: TypedPipeFactory[_] =>
        Some(tp.asInstanceOf[TypedPipeFactory[T]].next)
      case _ => None
    }
}

/**
 * This is a TypedPipe that delays having access
 * to the FlowDef and Mode until toPipe is called
 */
class TypedPipeFactory[T] private (@transient val next: NoStackAndThen[(FlowDef, Mode), TypedPipe[T]]) extends TypedPipe[T] {
  private[this] def andThen[U](fn: TypedPipe[T] => TypedPipe[U]): TypedPipe[U] =
    new TypedPipeFactory(next.andThen(fn))

  override def cross[U](tiny: TypedPipe[U]) = andThen(_.cross(tiny))
  override def filter(f: T => Boolean): TypedPipe[T] = andThen(_.filter(f))
  override def flatMap[U](f: T => TraversableOnce[U]): TypedPipe[U] = andThen(_.flatMap(f))
  override def map[U](f: T => U): TypedPipe[U] = andThen(_.map(f))

  override def sumByLocalKeys[K, V](implicit ev: T <:< (K, V), sg: Semigroup[V]) =
    andThen(_.sumByLocalKeys[K, V])

  override def asPipe[U >: T](fieldNames: Fields)(implicit flowDef: FlowDef, mode: Mode, setter: TupleSetter[U]) = {
    // unwrap in a loop, without recursing
    val (unwrapped, st) = unwrap(this, Array())
    val pipe = unwrapped.asPipe[U](fieldNames)(flowDef, mode, setter)
    RichPipe.setPipeDescriptionFrom(pipe, LineNumber.tryNonScaldingCaller(st))
    pipe
  }

  override def toIterableExecution: Execution[Iterable[T]] = Execution.getConfigMode.flatMap {
    case (conf, mode) =>
      // This can only terminate in TypedPipeInst, which will
      // keep the reference to this flowDef
      val flowDef = new FlowDef
      val (nextPipe, stackTraces) = unwrap(this, Array())(flowDef, mode)
      nextPipe.toIterableExecution
  }

  @annotation.tailrec
  private def unwrap(pipe: TypedPipe[T], st: Array[StackTraceElement])(implicit flowDef: FlowDef, mode: Mode): (TypedPipe[T], Array[StackTraceElement]) = pipe match {
    case TypedPipeFactory(n) =>
      val fullTrace = n match {
        case NoStackAndThen.WithStackTrace(_, st) => st
        case _ => Array[StackTraceElement]()
      }
      unwrap(n(flowDef, mode), st ++ fullTrace)
    case tp => (tp, st)
  }
}

/**
 * This is an instance of a TypedPipe that wraps a cascading Pipe
 */
class TypedPipeInst[T] private[scalding] (@transient inpipe: Pipe,
  fields: Fields,
  @transient localFlowDef: FlowDef,
  @transient val mode: Mode,
  flatMapFn: FlatMapFn[T]) extends TypedPipe[T] {

  /**
   * If this TypedPipeInst represents a Source that was opened with no
   * filtering or mapping
   */
  private[scalding] def openIfHead: Option[(Tap[_, _, _], Fields, FlatMapFn[T])] =
    // Keep this local
    if (inpipe.getPrevious.isEmpty) {
      val srcs = localFlowDef.getSources
      if (srcs.containsKey(inpipe.getName)) {
        Some((srcs.get(inpipe.getName), fields, flatMapFn))
      } else {
        sys.error("Invalid head: pipe has no previous, but there is no registered source.")
      }
    } else None

  def checkMode(m: Mode): Unit =
    // This check is not likely to fail unless someone does something really strange.
    // for historical reasons, it is not checked by the typed system
    assert(m == mode,
      "Cannot switch Mode between TypedSource.read and toPipe calls. Pipe: %s, call: %s".format(mode, m))

  override def cross[U](tiny: TypedPipe[U]): TypedPipe[(T, U)] = tiny match {
    case EmptyTypedPipe => EmptyTypedPipe
    case MergedTypedPipe(l, r) => MergedTypedPipe(cross(l), cross(r))
    case IterablePipe(iter) => flatMap { t => iter.map { (t, _) } }
    // This should work for any, TODO, should we just call this?
    case _ => map(((), _)).hashJoin(tiny.groupAll).values
  }

  override def filter(f: T => Boolean): TypedPipe[T] =
    new TypedPipeInst[T](inpipe, fields, localFlowDef, mode, flatMapFn.filter(f))

  override def flatMap[U](f: T => TraversableOnce[U]): TypedPipe[U] =
    new TypedPipeInst[U](inpipe, fields, localFlowDef, mode, flatMapFn.flatMap(f))

  override def map[U](f: T => U): TypedPipe[U] =
    new TypedPipeInst[U](inpipe, fields, localFlowDef, mode, flatMapFn.map(f))

  /**
   * Avoid this method if possible. Prefer to stay in the TypedAPI until
   * you write out.
   *
   * This actually runs all the pure map functions in one Cascading Each
   * This approach is more efficient than untyped scalding because we
   * don't use TupleConverters/Setters after each map.
   */
  override def asPipe[U >: T](fieldNames: Fields)(implicit flowDef: FlowDef, m: Mode, setter: TupleSetter[U]): Pipe = {
    import Dsl.flowDefToRichFlowDef
    checkMode(m)
    flowDef.mergeFrom(localFlowDef)
    RichPipe(inpipe).flatMapTo[TupleEntry, U](fields -> fieldNames)(flatMapFn)
  }

  override def sumByLocalKeys[K, V](implicit ev: T <:< (K, V), sg: Semigroup[V]): TypedPipe[(K, V)] = {
    import Dsl.{ fields => ofields, _ }
    val destFields: Fields = ('key, 'value)
    val selfKV = raiseTo[(K, V)]

    val msr = new TypedMapsideReduce[K, V](
      flatMapFn.asInstanceOf[FlatMapFn[(K, V)]],
      sg,
      fields,
      'key,
      'value,
      None)(tup2Setter)
    TypedPipe.from[(K, V)](
      inpipe.eachTo(fields -> destFields) { _ => msr },
      destFields)(localFlowDef, mode, tuple2Converter)
  }

  override def toIterableExecution: Execution[Iterable[T]] =
    openIfHead match {
      // TODO: it might be good to apply flatMaps locally,
      // since we obviously need to iterate all,
      // but filters we might want the cluster to apply
      // for us. So unwind until you hit the first filter, snapshot,
      // then apply the unwound functions
      case Some((tap, fields, Converter(conv))) =>
        // To convert from java iterator to scala below
        import scala.collection.JavaConverters._
        Execution.getConfigMode.map {
          case (conf, m) =>
            // Verify the mode has not changed due to invalid TypedPipe DAG construction
            checkMode(m)
            new Iterable[T] {
              def iterator = m.openForRead(conf, tap).asScala.map(tup => conv(tup.selectEntry(fields)))
            }
        }
      case _ => forceToDiskExecution.flatMap(_.toIterableExecution)
    }
}

final case class MergedTypedPipe[T](left: TypedPipe[T], right: TypedPipe[T]) extends TypedPipe[T] {

  override def cross[U](tiny: TypedPipe[U]): TypedPipe[(T, U)] = tiny match {
    case EmptyTypedPipe => EmptyTypedPipe
    case _ => MergedTypedPipe(left.cross(tiny), right.cross(tiny))
  }

  override def debug: TypedPipe[T] =
    MergedTypedPipe(left.debug, right.debug)

  override def filter(f: T => Boolean): TypedPipe[T] =
    MergedTypedPipe(left.filter(f), right.filter(f))

  override def flatMap[U](f: T => TraversableOnce[U]): TypedPipe[U] =
    MergedTypedPipe(left.flatMap(f), right.flatMap(f))

  override def sample(fraction: Double, seed: Long): TypedPipe[T] =
    MergedTypedPipe(left.sample(fraction, seed), right.sample(fraction, seed))

  override def sumByLocalKeys[K, V](implicit ev: T <:< (K, V), sg: Semigroup[V]): TypedPipe[(K, V)] =
    MergedTypedPipe(left.sumByLocalKeys, right.sumByLocalKeys)

  override def map[U](f: T => U): TypedPipe[U] =
    MergedTypedPipe(left.map(f), right.map(f))

  override def fork: TypedPipe[T] =
    MergedTypedPipe(left.fork, right.fork)

  @annotation.tailrec
  private def flattenMerge(toFlatten: List[TypedPipe[T]], acc: List[TypedPipe[T]])(implicit fd: FlowDef, m: Mode): List[TypedPipe[T]] =
    toFlatten match {
      case MergedTypedPipe(l, r) :: rest => flattenMerge(l :: r :: rest, acc)
      case TypedPipeFactory(next) :: rest => flattenMerge(next(fd, m) :: rest, acc)
      case nonmerge :: rest => flattenMerge(rest, nonmerge :: acc)
      case Nil => acc
    }

  override def asPipe[U >: T](fieldNames: Fields)(implicit flowDef: FlowDef, mode: Mode, setter: TupleSetter[U]): Pipe = {
    /*
     * Cascading can't handle duplicate pipes in merges. What we do here is see if any pipe appears
     * multiple times and if it does we can do self merges using flatMap.
     * Finally, if there is actually more than one distinct TypedPipe, we use the cascading
     * merge primitive. When using the merge primitive we rename all pipes going into it as
     * Cascading cannot handle multiple pipes with the same name.
     */
    val merged = flattenMerge(List(this), Nil)
      // check for repeated pipes
      .groupBy(identity)
      .mapValues(_.size)
      .map {
        case (pipe, 1) => pipe
        case (pipe, cnt) => pipe.flatMap(List.fill(cnt)(_).iterator)
      }
      .map(_.toPipe[U](fieldNames)(flowDef, mode, setter)) // linter:ignore
      .toList

    if (merged.size == 1) {
      // there is no actual merging here, no need to rename:
      merged.head
    } else {
      new cascading.pipe.Merge(merged.map(RichPipe.assignName): _*)
    }
  }

  override def hashCogroup[K, V, W, R](smaller: HashJoinable[K, W])(joiner: (K, V, Iterable[W]) => Iterator[R])(implicit ev: TypedPipe[T] <:< TypedPipe[(K, V)]): TypedPipe[(K, R)] =
    MergedTypedPipe(left.hashCogroup(smaller)(joiner), right.hashCogroup(smaller)(joiner))
}

case class WithOnComplete[T](typedPipe: TypedPipe[T], fn: () => Unit) extends TypedPipe[T] {
  override def asPipe[U >: T](fieldNames: Fields)(implicit flowDef: FlowDef, mode: Mode, setter: TupleSetter[U]) = {
    val pipe = typedPipe.toPipe[U](fieldNames)(flowDef, mode, setter)
    new Each(pipe, Fields.ALL, new CleanupIdentityFunction(fn), Fields.REPLACE)
  }
  override def cross[U](tiny: TypedPipe[U]): TypedPipe[(T, U)] =
    WithOnComplete(typedPipe.cross(tiny), fn)
  override def flatMap[U](f: T => TraversableOnce[U]): TypedPipe[U] =
    WithOnComplete(typedPipe.flatMap(f), fn)
}

case class WithDescriptionTypedPipe[T](typedPipe: TypedPipe[T], description: String) extends TypedPipe[T] {
  override def asPipe[U >: T](fieldNames: Fields)(implicit flowDef: FlowDef, mode: Mode, setter: TupleSetter[U]) = {
    val pipe = typedPipe.toPipe[U](fieldNames)(flowDef, mode, setter)
    RichPipe.setPipeDescriptions(pipe, List(description))
  }
  override def cross[U](tiny: TypedPipe[U]): TypedPipe[(T, U)] =
    WithDescriptionTypedPipe(typedPipe.cross(tiny), description)
  override def flatMap[U](f: T => TraversableOnce[U]): TypedPipe[U] =
    WithDescriptionTypedPipe(typedPipe.flatMap(f), description)
}

/**
 * This class is for the syntax enrichment enabling
 * .joinBy on TypedPipes. To access this, do
 * import Syntax.joinOnMappablePipe
 */
class MappablePipeJoinEnrichment[T](pipe: TypedPipe[T]) {
  def joinBy[K, U](smaller: TypedPipe[U])(g: (T => K), h: (U => K), reducers: Int = -1)(implicit ord: Ordering[K]): CoGrouped[K, (T, U)] = pipe.groupBy(g).withReducers(reducers).join(smaller.groupBy(h))
  def leftJoinBy[K, U](smaller: TypedPipe[U])(g: (T => K), h: (U => K), reducers: Int = -1)(implicit ord: Ordering[K]): CoGrouped[K, (T, Option[U])] = pipe.groupBy(g).withReducers(reducers).leftJoin(smaller.groupBy(h))
  def rightJoinBy[K, U](smaller: TypedPipe[U])(g: (T => K), h: (U => K), reducers: Int = -1)(implicit ord: Ordering[K]): CoGrouped[K, (Option[T], U)] = pipe.groupBy(g).withReducers(reducers).rightJoin(smaller.groupBy(h))
  def outerJoinBy[K, U](smaller: TypedPipe[U])(g: (T => K), h: (U => K), reducers: Int = -1)(implicit ord: Ordering[K]): CoGrouped[K, (Option[T], Option[U])] = pipe.groupBy(g).withReducers(reducers).outerJoin(smaller.groupBy(h))
}

/**
 * These are named syntax extensions that users can optionally import.
 * Avoid import Syntax._
 */
object Syntax {
  implicit def joinOnMappablePipe[T](p: TypedPipe[T]): MappablePipeJoinEnrichment[T] = new MappablePipeJoinEnrichment(p)
}
