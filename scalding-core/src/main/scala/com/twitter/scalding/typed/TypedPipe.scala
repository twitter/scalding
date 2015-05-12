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

import java.io.Serializable

import com.twitter.algebird.{ Semigroup, Monoid, Ring, Aggregator }

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.twitter.scalding._
import scala.reflect.ClassTag
import java.util.Random // prefer to scala.util.Random as this is serializable
import com.twitter.algebird._
import scala.concurrent.Future

/**
 * factory methods for TypedPipe, which is the typed representation of distributed lists in scalding.
 * This object is here rather than in the typed package because a lot of code was written using
 * the functions in the object, which we do not see how to hide with package object tricks.
 */
object TypedPipe extends Serializable {

  /**
   * Create a TypedPipe from a cascading Pipe, some Fields and the type T
   * Avoid this if you can. Prefer from(TypedSource).
   */
  def from[T](rdd: RDD[T])(implicit sc: SparkContext, mode: Mode): TypedPipe[T] = {
    new TypedPipeInst[T, T](rdd, sc, mode, NoOpFlatMapFn[T])
  }

  // /**
  //  * Create a TypedPipe from a TypedSource. This is the preferred way to make a TypedPipe
  //  */
  // def from[T](source: TypedSource[T]): TypedPipe[T] =
  //   TypedPipeFactory({ (fd, mode) =>
  //     val pipe = source.read(fd, mode)
  //     from(pipe, source.sourceFields)(fd, mode, source.converter)
  //   })

  /**
   * Create a TypedPipe from an Iterable in memory.
   */
  def from[T: ClassTag](iter: Iterable[T])(implicit sc: SparkContext, mode: Mode): TypedPipe[T] =
    from(sc.parallelize(iter.toSeq, 1))

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
      // def joinFunction = CoGroupable.castingJoinFunction[V]
    }

  /**
   * TypedPipe instances are monoids. They are isomorphic to multisets.
   */
  implicit def typedPipeMonoid[T]: Monoid[TypedPipe[T]] = new Monoid[TypedPipe[T]] {
    def zero = empty
    def plus(left: TypedPipe[T], right: TypedPipe[T]): TypedPipe[T] =
      left ++ right
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
  def cross[W >: T: ClassTag, V: ClassTag](tiny: TypedPipe[V]): TypedPipe[(W, V)]

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
  def toRDD[U >: T: ClassTag]()(implicit sc: SparkContext, mode: Mode): RDD[U]

  /////////////////////////////////////////////
  //
  // The following have default implementations in terms of the above
  //
  /////////////////////////////////////////////

  def collect[U >: T: ClassTag]()(implicit sc: SparkContext, mode: Mode): Seq[U] =
    toRDD[U].collect

  def dump[U >: T: ClassTag]()(implicit sc: SparkContext, mode: Mode): Unit =
    collect[U].foreach(println)

  /**
   * Merge two TypedPipes (no order is guaranteed)
   * This is only realized when a group (or join) is
   * performed.
   */
  def ++[U >: T](other: TypedPipe[U]): TypedPipe[U] = other match {
    case EmptyTypedPipe => this
    case _ => MergedTypedPipe(this, other)
  }

  // /**
  //  * Aggregate all items in this pipe into a single ValuePipe
  //  *
  //  * Aggregators are composable reductions that allow you to glue together
  //  * several reductions and process them in one pass.
  //  *
  //  * Same as groupAll.aggregate.values
  //  */
  // def aggregate[B, C](agg: Aggregator[T, B, C]): ValuePipe[C] =
  //   ComputedValue(groupAll.aggregate(agg).values)

  /**
   * Put the items in this into the keys, and unit as the value in a Group
   * in some sense, this is the dual of groupAll
   */
  @annotation.implicitNotFound(msg = "For asKeys method to work, the type in TypedPipe must have an Ordering.")
  def asKeys[U >: T: ClassTag](implicit ord: Ordering[U]): Grouped[U, Unit] =
    map((_, ())).group[U, Unit]

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

  // /**
  //  * Attach a ValuePipe to each element this TypedPipe
  //  */
  // def cross[V](p: ValuePipe[V]): TypedPipe[(T, V)] =
  //   p match {
  //     case EmptyValue => EmptyTypedPipe
  //     case LiteralValue(v) => map { (_, v) }
  //     case ComputedValue(pipe) => cross(pipe)
  //   }

  /** prints the current pipe to stdout */
  def debug[U >: T: ClassTag]: TypedPipe[U] =
    TypedPipeFactory({ (sc, m) =>
      implicit val sparkContext = sc
      implicit val mode = m
      val rdd = raiseTo[U].toRDD
      TypedPipe.from(rdd.map{ x =>
        println(x)
        x
      })
    })

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
  def distinct[U >: T: ClassTag](implicit ord: Ordering[_ >: U]): TypedPipe[U] =
    asKeys(implicitly[ClassTag[U]], ord.asInstanceOf[Ordering[U]]).sum.keys

  /**
   * Returns the set of distinct elements identified by a given lambda extractor in the TypedPipe
   */
  @annotation.implicitNotFound(msg = "For distinctBy method to work, the type to distinct on in the TypedPipe must have an Ordering.")
  def distinctBy[U >: T: ClassTag, V: ClassTag](fn: U => V, numReducers: Option[Int] = None)(implicit ord: Ordering[_ >: V]): TypedPipe[U] = {
    // cast because Ordering is not contravariant, but should be (and this cast is safe)
    implicit val ordT: Ordering[V] = ord.asInstanceOf[Ordering[V]]

    // Semigroup to handle duplicates for a given key might have different values.
    implicit val sg = new Semigroup[U] {
      def plus(a: U, b: U) = b
    }

    val op = map{ tup => (fn(tup), tup) }.sumByKey[V, U]
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
  def fork[U >: T: ClassTag]: TypedPipe[U] =
    TypedPipeFactory({ (sc, m) =>
      implicit val sparkContext = sc
      implicit val mode = m
      val rdd = raiseTo[U].toRDD
      TypedPipe.from(rdd)
    })

  // /**
  //  * WARNING This is dangerous, and may not be what you think.
  //  *
  //  * limit the output to AT MOST count items.
  //  * useful for debugging, but probably that's about it.
  //  * The number may be less than count, and not sampled by any particular method
  //  *
  //  * This may change in the future to be exact, but that will add 1 MR step
  //  */
  // def limit(count: Int): TypedPipe[T] = onRawSingle(_.limit(count))

  /** Transform each element via the function f */
  def map[U](f: T => U): TypedPipe[U] = flatMap { t => Iterator(f(t)) }

  /** Transform only the values (sometimes requires giving the types due to scala type inference) */
  def mapValues[K, V, U](f: V => U)(implicit ev: T <:< (K, V)): TypedPipe[(K, U)] =
    raiseTo[(K, V)].map { case (k, v) => (k, f(v)) }

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

  // protected def onRawSingle(onPipe: Pipe => Pipe): TypedPipe[T] = {
  //   val self = this
  //   TypedPipeFactory({ (fd, m) =>
  //     val pipe = self.toPipe[T](new Fields(java.lang.Integer.valueOf(0)))(fd, m, singleSetter)
  //     TypedPipe.fromSingleField[T](onPipe(pipe))(fd, m)
  //   })
  // }

  /**
   * Force a materialization of this pipe prior to the next operation.
   * This is useful if you filter almost everything before a hashJoin, for instance.
   * This is useful for experts who see some heuristic of the planner causing
   * slower performance.
   */
  def forceToDisk[U >: T: ClassTag]: TypedPipe[U] =
    TypedPipeFactory({ (sc, m) =>
      implicit val sparkContext = sc
      implicit val mode = m
      val rdd = raiseTo[U].toRDD
      rdd.checkpoint
      TypedPipe.from(rdd)
    })

  /**
   * This is the default means of grouping all pairs with the same key. Generally this triggers 1 Map/Reduce transition
   */
  def group[K: ClassTag, V: ClassTag](implicit ev: <:<[T, (K, V)], ord: Ordering[K]): Grouped[K, V] =
    //If the type of T is not (K,V), then at compile time, this will fail.  It uses implicits to do
    //a compile time check that one type is equivalent to another.  If T is not (K,V), we can't
    //automatically group.  We cast because it is safe to do so, and we need to convert to K,V, but
    //the ev is not needed for the cast.  In fact, you can do the cast with ev(t) and it will return
    //it as (K,V), but the problem is, ev is not serializable.  So we do the cast, which due to ev
    //being present, will always pass.
    Grouped(raiseTo[(K, V)])

  /** Send all items to a single reducer */
  def groupAll[U >: T: ClassTag]: Grouped[Unit, U] = groupBy[Unit, U](x => ()).withReducers(1)

  /** Given a key function, add the key, then call .group */
  def groupBy[K: ClassTag, U >: T: ClassTag](g: U => K)(implicit ord: Ordering[K]): Grouped[K, U] =
    map { t => (g(t), t) }.group

  /**
   * Forces a shuffle by randomly assigning each item into one
   * of the partitions.
   *
   * This is for the case where you mappers take a long time, and
   * it is faster to shuffle them to more reducers and then operate.
   *
   * You probably want shard if you are just forcing a shuffle.
   */
  def groupRandomly[U >: T: ClassTag](partitions: Int): Grouped[Int, U] = {
    // Make it lazy so all mappers get their own:
    lazy val rng = new java.util.Random(123) // seed this so it is repeatable
    groupBy[Int, U] { _ => rng.nextInt(partitions) }
      .withReducers(partitions)
  }

  /**
   * Partitions this into two pipes according to a predicate.
   *
   * Sometimes what you really want is a groupBy in these cases.
   */
  def partition[U >: T: ClassTag](p: U => Boolean): (TypedPipe[U], TypedPipe[U]) = {
    val forked = fork[U]
    (forked.filter(p), forked.filterNot(p))
  }

  // private[this] def defaultSeed: Long = System.identityHashCode(this) * 2654435761L ^ System.currentTimeMillis
  // /**
  //  * Sample uniformly independently at random each element of the pipe
  //  * does not require a reduce step.
  //  */
  // def sample(percent: Double): TypedPipe[T] = sample(percent, defaultSeed)
  // /**
  //  * Sample uniformly independently at random each element of the pipe with
  //  * a given seed.
  //  * Does not require a reduce step.
  //  */
  // def sample(percent: Double, seed: Long): TypedPipe[T] = {
  //   // Make sure to fix the seed, otherwise restarts cause subtle errors
  //   lazy val rand = new Random(seed)
  //   filter(_ => rand.nextDouble < percent)
  // }

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
    val selfKV = raiseTo[(K, V)]
    TypedPipeFactory({ (sc, m) =>
      implicit val sparkContext = sc
      implicit val mode = m
      val rdd = selfKV.toRDD
      // Default monoid is sparse
      val denseMapMonoid = new MapMonoid[K, V] {
        override val nonZero: (V => Boolean) = (_ => true)
      }

      TypedPipe.from(rdd.mapPartitions { f: Iterator[(K, V)] =>
        // TODO make me spill if necessary
        denseMapMonoid.sum(f.map(Map(_))).toIterator
      })
    })
  }

  /**
   * Used to force a shuffle into a given size of nodes.
   * Only use this if your mappers are taking far longer than
   * the time to shuffle.
   */
  def shard[U >: T: ClassTag](partitions: Int): TypedPipe[U] =
    TypedPipeFactory({ (sc, m) =>
      implicit val sparkContext = sc
      implicit val mode = m
      val rdd = raiseTo[U].toRDD
      TypedPipe.from(rdd.repartition(partitions))
    })

  // /**
  //  * Reasonably common shortcut for cases of total associative/commutative reduction
  //  * returns a ValuePipe with only one element if there is any input, otherwise EmptyValue.
  //  */
  // def sum[U >: T](implicit plus: Semigroup[U]): ValuePipe[U] = ComputedValue(groupAll.sum[U].values)

  /**
   * Reasonably common shortcut for cases of associative/commutative reduction by Key
   */
  def sumByKey[K: ClassTag, V: ClassTag](implicit ev: T <:< (K, V), ord: Ordering[K], plus: Semigroup[V]): UnsortedGrouped[K, V] =
    group[K, V].sum[V]

  // /**
  //  * This is used when you are working with Execution[T] to create loops.
  //  * You might do this to checkpoint and then flatMap Execution to continue
  //  * from there. Probably only useful if you need to flatMap it twice to fan
  //  * out the data into two children jobs.
  //  *
  //  * This writes the current TypedPipe into a temporary file
  //  * and then opens it after complete so that you can continue from that point
  //  */
  // def forceToDiskExecution: Execution[TypedPipe[T]] = Execution
  //   .getConfigMode
  //   .flatMap {
  //     case (conf, mode) =>
  //       mode match {
  //         case _: CascadingLocal => // Local or Test mode
  //           val dest = new MemorySink[T]
  //           writeExecution(dest).map { _ => TypedPipe.from(dest.readResults) }
  //         case _: HadoopMode =>
  //           // come up with unique temporary filename, use the config here
  //           // TODO: refactor into TemporarySequenceFile class
  //           val tmpDir = conf.get("hadoop.tmp.dir")
  //             .orElse(conf.get("cascading.tmp.dir"))
  //             .getOrElse("/tmp")

  //           val tmpSeq = tmpDir + "/scalding/snapshot-" + java.util.UUID.randomUUID + ".seq"
  //           val dest = source.TypedSequenceFile[T](tmpSeq)
  //           writeThrough(dest)
  //       }
  //   }

  // /**
  //  * This gives an Execution that when run evaluates the TypedPipe,
  //  * writes it to disk, and then gives you an Iterable that reads from
  //  * disk on the submit node each time .iterator is called.
  //  * Because of how scala Iterables work, mapping/flatMapping/filtering
  //  * the Iterable forces a read of the entire thing. If you need it to
  //  * be lazy, call .iterator and use the Iterator inside instead.
  //  */
  // def toIterableExecution: Execution[Iterable[T]]

  // /**
  //  * Safely write to a TypedSink[T]. If you want to write to a Source (not a Sink)
  //  * you need to do something like: toPipe(fieldNames).write(dest)
  //  * @return a pipe equivalent to the current pipe.
  //  */
  // def write(dest: TypedSink[T])(implicit flowDef: FlowDef, mode: Mode): TypedPipe[T] = {
  //   // Make sure that we don't render the whole pipeline twice:
  //   val res = fork
  //   dest.writeFrom(res.toPipe[T](dest.sinkFields)(flowDef, mode, dest.setter))
  //   res
  // }

  // /**
  //  * This is the functionally pure approach to building jobs. Note,
  //  * that you have to call run on the result or flatMap/zip it
  //  * into an Execution that is run for anything to happen here.
  //  */
  // def writeExecution(dest: TypedSink[T]): Execution[Unit] =
  //   Execution.write(this, dest)

  // /**
  //  * If you want to write to a specific location, and then read from
  //  * that location going forward, use this.
  //  */
  // def writeThrough[U >: T](dest: TypedSink[T] with TypedSource[U]): Execution[TypedPipe[U]] =
  //   writeExecution(dest)
  //     .map(_ => TypedPipe.from(dest))

  // /**
  //  * If you want to writeThrough to a specific file if it doesn't already exist,
  //  * and otherwise just read from it going forward, use this.
  //  */
  // def make[U >: T](dest: FileSource with TypedSink[T] with TypedSource[U]): Execution[TypedPipe[U]] =
  //   Execution.getMode.flatMap { mode =>
  //     try {
  //       dest.validateTaps(mode)
  //       Execution.from(TypedPipe.from(dest))
  //     } catch {
  //       case ivs: InvalidSourceException => writeThrough(dest)
  //     }
  //   }

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

  // /**
  //  * ValuePipe may be empty, so, this attaches it as an Option
  //  * cross is the same as leftCross(p).collect { case (t, Some(v)) => (t, v) }
  //  */
  // def leftCross[V](p: ValuePipe[V]): TypedPipe[(T, Option[V])] =
  //   p match {
  //     case EmptyValue => map { (_, None) }
  //     case LiteralValue(v) => map { (_, Some(v)) }
  //     case ComputedValue(pipe) => leftCross(pipe)
  //   }

  // /** uses hashJoin but attaches None if thatPipe is empty */
  // def leftCross[V](thatPipe: TypedPipe[V]): TypedPipe[(T, Option[V])] =
  //   map(((), _)).hashLeftJoin(thatPipe.groupAll).values

  // /**
  //  * common pattern of attaching a value and then map
  //  * recommended style:
  //  * {@code
  //  *  mapWithValue(vpu) {
  //  *    case (t, Some(u)) => op(t, u)
  //  *    case (t, None) => // if you never expect this:
  //  *      sys.error("unexpected empty value pipe")
  //  *  }
  //  * }
  //  */
  // def mapWithValue[U, V](value: ValuePipe[U])(f: (T, Option[U]) => V): TypedPipe[V] =
  //   leftCross(value).map(t => f(t._1, t._2))

  // /**
  //  * common pattern of attaching a value and then flatMap
  //  * recommended style:
  //  * {@code
  //  *  flatMapWithValue(vpu) {
  //  *    case (t, Some(u)) => op(t, u)
  //  *    case (t, None) => // if you never expect this:
  //  *      sys.error("unexpected empty value pipe")
  //  *  }
  //  * }
  //  */
  // def flatMapWithValue[U, V](value: ValuePipe[U])(f: (T, Option[U]) => TraversableOnce[V]): TypedPipe[V] =
  //   leftCross(value).flatMap(t => f(t._1, t._2))

  // /**
  //  * common pattern of attaching a value and then filter
  //  * recommended style:
  //  * {@code
  //  *  filterWithValue(vpu) {
  //  *    case (t, Some(u)) => op(t, u)
  //  *    case (t, None) => // if you never expect this:
  //  *      sys.error("unexpected empty value pipe")
  //  *  }
  //  * }
  //  */
  // def filterWithValue[U](value: ValuePipe[U])(f: (T, Option[U]) => Boolean): TypedPipe[T] =
  //   leftCross(value).filter(t => f(t._1, t._2)).map(_._1)

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
  def hashCogroup[K, V, W, R: ClassTag](smaller: HashJoinable[K, W])(joiner: (K, V, Iterable[W]) => Iterator[R])(implicit ev: TypedPipe[T] <:< TypedPipe[(K, V)]): TypedPipe[(K, R)] =
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

  // /**
  //  * Enables joining when this TypedPipe has some keys with many many values and
  //  * but many with very few values. For instance, a graph where some nodes have
  //  * millions of neighbors, but most have only a few.
  //  *
  //  * We build a (count-min) sketch of each key's frequency, and we use that
  //  * to shard the heavy keys across many reducers.
  //  * This increases communication cost in order to reduce the maximum time needed
  //  * to complete the join.
  //  *
  //  * {@code pipe.sketch(100).join(thatPipe) }
  //  * will add an extra map/reduce job over a standard join to create the count-min-sketch.
  //  * This will generally only be beneficial if you have really heavy skew, where without
  //  * this you have 1 or 2 reducers taking hours longer than the rest.
  //  */
  // def sketch[K, V](reducers: Int,
  //   eps: Double = 1.0E-5, //272k width = 1MB per row
  //   delta: Double = 0.01, //5 rows (= 5 hashes)
  //   seed: Int = 12345)(implicit ev: TypedPipe[T] <:< TypedPipe[(K, V)],
  //     serialization: K => Array[Byte],
  //     ordering: Ordering[K]): Sketched[K, V] =
  //   Sketched(ev(this), reducers, delta, eps, seed)

}

/**
 * This object is the EmptyTypedPipe. Prefer to create it with TypedPipe.empty
 */
final case object EmptyTypedPipe extends TypedPipe[Nothing] {

  // override def aggregate[B, C](agg: Aggregator[Nothing, B, C]): ValuePipe[C] = EmptyValue

  // Cross product with empty is always empty.
  override def cross[W >: Nothing: ClassTag, V: ClassTag](tiny: TypedPipe[V]): TypedPipe[(Nothing, V)] = this

  override def distinct[U >: Nothing: ClassTag](implicit ord: Ordering[_ >: U]): TypedPipe[U] = this

  override def flatMap[U](f: Nothing => TraversableOnce[U]) = this

  // override def fork: TypedPipe[Nothing] = this

  override def forceToDisk[U >: Nothing: ClassTag]: TypedPipe[U] = this

  // override def leftCross[V](p: ValuePipe[V]) = this

  // override def limit(count: Int) = this

  // override def ++[U >: Nothing](other: TypedPipe[U]): TypedPipe[U] = other

  override def toRDD[R >: Nothing: ClassTag]()(implicit newSc: SparkContext, mode: Mode): RDD[R] = {
    newSc.parallelize(Seq[R]())
  }

  // override def toIterableExecution: Execution[Iterable[Nothing]] = Execution.from(Iterable.empty)

  // override def forceToDiskExecution: Execution[TypedPipe[Nothing]] = Execution.from(this)

  // override def sum[U >: Nothing](implicit plus: Semigroup[U]): ValuePipe[U] = EmptyValue

  override def sumByLocalKeys[K, V](implicit ev: Nothing <:< (K, V), sg: Semigroup[V]) = this

  override def hashCogroup[K, V, W, R: ClassTag](smaller: HashJoinable[K, W])(joiner: (K, V, Iterable[W]) => Iterator[R])(implicit ev: TypedPipe[Nothing] <:< TypedPipe[(K, V)]): TypedPipe[(K, R)] =
    this
}

/**
 * This is an implementation detail (and should be marked private)
 */
object TypedPipeFactory {
  def apply[T](next: (SparkContext, Mode) => TypedPipe[T]): TypedPipeFactory[T] = {
    val memo = new java.util.WeakHashMap[SparkContext, (Mode, TypedPipe[T])]()
    val fn = { (fd: SparkContext, m: Mode) =>
      memo.synchronized {
        memo.get(fd) match {
          case null =>
            val res = next(fd, m)
            memo.put(fd, (m, res))
            res
          case (memoMode, pipe) if memoMode == m => pipe
          case (memoMode, pipe) =>
            sys.error("SparkContext reused on different Mode. Original: %s, now: %s".format(memoMode, m))
        }
      }
    }
    new TypedPipeFactory(NoStackAndThen(fn.tupled))
  }
  def unapply[T](tp: TypedPipe[T]): Option[NoStackAndThen[(SparkContext, Mode), TypedPipe[T]]] =
    tp match {
      case tp: TypedPipeFactory[_] =>
        Some(tp.asInstanceOf[TypedPipeFactory[T]].next)
      case _ => None
    }
}

class TypedPipeFactory[T] private (@transient val next: NoStackAndThen[(SparkContext, Mode), TypedPipe[T]]) extends TypedPipe[T] {
  private[this] def andThen[U](fn: TypedPipe[T] => TypedPipe[U]): TypedPipe[U] =
    new TypedPipeFactory(next.andThen(fn))

  override def cross[W >: T: ClassTag, V: ClassTag](tiny: TypedPipe[V]) = andThen(_.cross[W, V](tiny))
  override def filter(f: T => Boolean): TypedPipe[T] = andThen(_.filter(f))
  override def flatMap[U](f: T => TraversableOnce[U]): TypedPipe[U] = andThen(_.flatMap(f))
  override def map[U](f: T => U): TypedPipe[U] = andThen(_.map(f))

  // override def limit(count: Int) = andThen(_.limit(count))

  override def sumByLocalKeys[K, V](implicit ev: T <:< (K, V), sg: Semigroup[V]) =
    andThen(_.sumByLocalKeys[K, V])

  override def toRDD[R >: T: ClassTag]()(implicit newSc: SparkContext, mode: Mode): RDD[R] =
    unwrap(this).toRDD[R]

  // override def toIterableExecution: Execution[Iterable[T]] = Execution.getConfigMode.flatMap {
  //   case (conf, mode) =>
  //     // This can only terminate in TypedPipeInst, which will
  //     // keep the reference to this flowDef
  //     val flowDef = new FlowDef
  //     val nextPipe = unwrap(this)(flowDef, mode)
  //     nextPipe.toIterableExecution
  // }

  @annotation.tailrec
  private def unwrap(pipe: TypedPipe[T])(implicit sc: SparkContext, mode: Mode): TypedPipe[T] = pipe match {
    case TypedPipeFactory(n) => unwrap(n(sc, mode))
    case tp => tp
  }
}

/**
 * This is an instance of a TypedPipe that wraps a Spark RDD
 */
class TypedPipeInst[T, U] private[scalding] (@transient rdd: RDD[T],
  @transient sc: SparkContext,
  @transient val mode: Mode,
  flatMapFn: FlatMapFn[T, U]) extends TypedPipe[U] {

  // /**
  //  * If this TypedPipeInst represents a Source that was opened with no
  //  * filtering or mapping
  //  */
  // private[scalding] def openIfHead: Option[(Tap[_, _, _], Fields, FlatMapFn[T])] =
  //   // Keep this local
  //   if (inpipe.getPrevious.isEmpty) {
  //     val srcs = localFlowDef.getSources
  //     if (srcs.containsKey(inpipe.getName)) {
  //       Some((srcs.get(inpipe.getName), fields, flatMapFn))
  //     } else {
  //       sys.error("Invalid head: pipe has no previous, but there is no registered source.")
  //     }
  //   } else None

  def checkSc(s: SparkContext): Unit =
    // This check is not likely to fail unless someone does something really strange.
    // for historical reasons, it is not checked by the typed system
    assert(sc == s,
      "Cannot switch SparkContext between TypedSource.read and toPipe calls. RDD: %s, call: %s".format(sc, s))

  def checkMode(m: Mode): Unit =
    // This check is not likely to fail unless someone does something really strange.
    // for historical reasons, it is not checked by the typed system
    assert(m == mode,
      "Cannot switch Mode between TypedSource.read and toPipe calls. RDD: %s, call: %s".format(mode, m))

  override def cross[W >: U: ClassTag, V: ClassTag](tiny: TypedPipe[V]): TypedPipe[(W, V)] = tiny match {
    case EmptyTypedPipe => EmptyTypedPipe
    case MergedTypedPipe(l, r) => MergedTypedPipe(cross[W, V](l), cross[W, V](r))
    // This should work for any, TODO, should we just call this?
    case _ => map(((), _)).hashJoin[Unit, W, V](tiny.groupAll[V]).values
  }

  override def filter(f: U => Boolean): TypedPipe[U] =
    new TypedPipeInst[T, U](rdd, sc, mode, flatMapFn.filter(f))

  override def flatMap[R](f: U => TraversableOnce[R]): TypedPipe[R] =
    new TypedPipeInst[T, R](rdd, sc, mode, flatMapFn.flatMap(f))

  override def map[R](f: U => R): TypedPipe[R] =
    new TypedPipeInst[T, R](rdd, sc, mode, flatMapFn.map(f))

  /**
   * Avoid this method if possible. Prefer to stay in the TypedAPI until
   * you write out.
   *
   */
  override def toRDD[R >: U: ClassTag]()(implicit newSc: SparkContext, mode: Mode): RDD[R] = {
    checkMode(mode)
    checkSc(newSc)
    rdd.flatMap(flatMapFn)
  }

  // override def toIterableExecution: Execution[Iterable[T]] =
  //   openIfHead match {
  //     // TODO: it might be good to apply flatMaps locally,
  //     // since we obviously need to iterate all,
  //     // but filters we might want the cluster to apply
  //     // for us. So unwind until you hit the first filter, snapshot,
  //     // then apply the unwound functions
  //     case Some((tap, fields, Converter(conv))) =>
  //       // To convert from java iterator to scala below
  //       import scala.collection.JavaConverters._
  //       Execution.getConfigMode.map {
  //         case (conf, m) =>
  //           // Verify the mode has not changed due to invalid TypedPipe DAG construction
  //           checkMode(m)
  //           new Iterable[T] {
  //             def iterator = m.openForRead(conf, tap).asScala.map(tup => conv(tup.selectEntry(fields)))
  //           }
  //       }
  //     case _ => forceToDiskExecution.flatMap(_.toIterableExecution)
  //   }
}

final case class MergedTypedPipe[T](left: TypedPipe[T], right: TypedPipe[T]) extends TypedPipe[T] {

  override def cross[W >: T: ClassTag, V: ClassTag](tiny: TypedPipe[V]): TypedPipe[(W, V)] = tiny match {
    case EmptyTypedPipe => EmptyTypedPipe
    case _ => MergedTypedPipe(left.cross[W, V](tiny), right.cross[W, V](tiny))
  }

  override def filter(f: T => Boolean): TypedPipe[T] =
    MergedTypedPipe(left.filter(f), right.filter(f))

  override def flatMap[U](f: T => TraversableOnce[U]): TypedPipe[U] =
    MergedTypedPipe(left.flatMap(f), right.flatMap(f))

  // override def sample(percent: Double, seed: Long): TypedPipe[T] =
  //   MergedTypedPipe(left.sample(percent, seed), right.sample(percent, seed))

  override def sumByLocalKeys[K, V](implicit ev: T <:< (K, V), sg: Semigroup[V]): TypedPipe[(K, V)] =
    MergedTypedPipe(left.sumByLocalKeys, right.sumByLocalKeys)

  override def map[U](f: T => U): TypedPipe[U] =
    MergedTypedPipe(left.map(f), right.map(f))

  // override def fork: TypedPipe[T] =
  //   MergedTypedPipe(left.fork, right.fork)

  @annotation.tailrec
  private def flattenMerge(toFlatten: List[TypedPipe[T]], acc: List[TypedPipe[T]])(implicit sc: SparkContext, m: Mode): List[TypedPipe[T]] =
    toFlatten match {
      case MergedTypedPipe(l, r) :: rest => flattenMerge(l :: r :: rest, acc)
      case TypedPipeFactory(next) :: rest => flattenMerge(next(sc, m) :: rest, acc)
      case nonmerge :: rest => flattenMerge(rest, nonmerge :: acc)
      case Nil => acc
    }

  override def toRDD[R >: T: ClassTag]()(implicit newSc: SparkContext, mode: Mode): RDD[R] = {
    newSc.union(flattenMerge(List(this), Nil)
      .map(_.toRDD[R])
      .toList)

  }

  // override def toIterableExecution: Execution[Iterable[T]] = forceToDiskExecution.flatMap(_.toIterableExecution)
  // override def hashCogroup[K, V, W, R](smaller: HashJoinable[K, W])(joiner: (K, V, Iterable[W]) => Iterator[R])(implicit ev: TypedPipe[T] <:< TypedPipe[(K, V)]): TypedPipe[(K, R)] =
  //   MergedTypedPipe(left.hashCogroup(smaller)(joiner), right.hashCogroup(smaller)(joiner))
}

// /**
//  * This class is for the syntax enrichment enabling
//  * .joinBy on TypedPipes. To access this, do
//  * import Syntax.joinOnMappablePipe
//  */
// class MappablePipeJoinEnrichment[T](pipe: TypedPipe[T]) {
//   def joinBy[K, U](smaller: TypedPipe[U])(g: (T => K), h: (U => K), reducers: Int = -1)(implicit ord: Ordering[K]): CoGrouped[K, (T, U)] = pipe.groupBy(g).withReducers(reducers).join(smaller.groupBy(h))
//   def leftJoinBy[K, U](smaller: TypedPipe[U])(g: (T => K), h: (U => K), reducers: Int = -1)(implicit ord: Ordering[K]): CoGrouped[K, (T, Option[U])] = pipe.groupBy(g).withReducers(reducers).leftJoin(smaller.groupBy(h))
//   def rightJoinBy[K, U](smaller: TypedPipe[U])(g: (T => K), h: (U => K), reducers: Int = -1)(implicit ord: Ordering[K]): CoGrouped[K, (Option[T], U)] = pipe.groupBy(g).withReducers(reducers).rightJoin(smaller.groupBy(h))
//   def outerJoinBy[K, U](smaller: TypedPipe[U])(g: (T => K), h: (U => K), reducers: Int = -1)(implicit ord: Ordering[K]): CoGrouped[K, (Option[T], Option[U])] = pipe.groupBy(g).withReducers(reducers).outerJoin(smaller.groupBy(h))
// }

// /**
//  * These are named syntax extensions that users can optionally import.
//  * Avoid import Syntax._
//  */
// object Syntax {
//   implicit def joinOnMappablePipe[T](p: TypedPipe[T]): MappablePipeJoinEnrichment[T] = new MappablePipeJoinEnrichment(p)
// }
