package com.twitter.scalding

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.tuple.Fields
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry

import java.io.Serializable

import com.twitter.algebird.{Semigroup, Ring, Aggregator}
import com.twitter.scalding.typed.{Joiner, CoGrouped2, HashCoGrouped2}

import com.twitter.scalding.TupleConverter.{singleConverter, tuple2Converter, CTupleConverter, TupleEntryConverter}
import com.twitter.scalding.TupleSetter.{singleSetter, tup2Setter}

/***************
** WARNING: This is a new an experimental API.  Expect API breaks.  If you want
** to be conservative, use the fields-based, standard scalding DSL.  This is attempting
** to be a type-safe DSL for cascading, that is closer to scoobi, spark and scrunch
****************/

/** implicits for the type-safe DSL
 * import TDsl._ to get the implicit conversions from Grouping/CoGrouping to Pipe,
 *   to get the .toTypedPipe method on standard cascading Pipes.
 *   to get automatic conversion of Mappable[T] to TypedPipe[T]
 */
object TDsl extends Serializable with GeneratedTupleAdders {
  implicit def pipeTExtensions(pipe : Pipe) : PipeTExtensions = new PipeTExtensions(pipe)

  implicit def mappableToTypedPipe[T](src: Mappable[T])
    (implicit flowDef: FlowDef, mode: Mode): TypedPipe[T] =
    TypedPipe.from(src)(flowDef, mode)

  implicit def sourceToTypedPipe[T](src: TypedSource[T])
    (implicit flowDef: FlowDef, mode: Mode): TypedPipe[T] =
    TypedPipe.from(src)(flowDef, mode)
}

/*
 * This is a type-class pattern of adding methods to Pipe relevant to TypedPipe
 */
class PipeTExtensions(pipe : Pipe) extends Serializable {
  /* Give you a syntax (you must put the full type on the TypedPipe, else type inference fails
   *   pipe.typed(('in0, 'in1) -> 'out) { tpipe : TypedPipe[(Int,Int)] =>
   *    // let's group all:
   *     tpipe.groupBy { x => 1 }
   *       .mapValues { tup => tup._1 + tup._2 }
   *       .sum
   *       .map { _._2 } //discard the key value, which is 1.
   *   }
   *  The above sums all the tuples and returns a TypedPipe[Int] which has the total sum.
   */
  def typed[T,U](fielddef : (Fields, Fields))(fn : TypedPipe[T] => TypedPipe[U])
    (implicit conv : TupleConverter[T], setter : TupleSetter[U]) : Pipe = {
    fn(TypedPipe.from(pipe, fielddef._1)(conv)).toPipe(fielddef._2)(setter)
  }
  def toTypedPipe[T](fields : Fields)(implicit conv : TupleConverter[T]) : TypedPipe[T] = {
    TypedPipe.from[T](pipe, fields)(conv)
  }
  def packToTypedPipe[T](fields : Fields)(implicit tp : TuplePacker[T]) : TypedPipe[T] = {
    val conv = tp.newConverter(fields)
    toTypedPipe(fields)(conv)
  }
}

/** factory methods for TypedPipe
 */
object TypedPipe extends Serializable {
  def from[T](pipe: Pipe, fields: Fields)(implicit conv: TupleConverter[T]): TypedPipe[T] =
    new TypedPipe[T](pipe, fields, {te => Some(conv(te))})

  def from[T](mappable: TypedSource[T])(implicit flowDef: FlowDef, mode: Mode) =
    new TypedPipe[T](mappable.read, mappable.sourceFields, {te => Some(mappable.converter(te))})
}

/** Represents a phase in a distributed computation on an input data source
 * Wraps a cascading Pipe object, and holds the transformation done up until that point
 */
class TypedPipe[+T] private (inpipe : Pipe, fields : Fields, flatMapFn : (TupleEntry) => TraversableOnce[T])
  extends Serializable {
  import Dsl._

  /** This actually runs all the pure map functions in one Cascading Each
   * This approach is more efficient than untyped scalding because we
   * don't use TupleConverters/Setters after each map.
   * The output pipe has a single item CTuple with an object of type T in position 0
   */
  protected lazy val pipe: Pipe =
    inpipe.flatMapTo(fields -> 0)(flatMapFn)(TupleEntryConverter, singleSetter[T])

  /** Same as groupAll.aggregate.values
   */
  def aggregate[B,C](agg: Aggregator[T,B,C]): TypedPipe[C] = groupAll.aggregate(agg).values

  // Implements a cross project.  The right side should be tiny
  def cross[U](tiny : TypedPipe[U]) : TypedPipe[(T,U)] = {
    val crossedPipe = pipe.rename(0 -> 't)
      .crossWithTiny(tiny.pipe.rename(0 -> 'u))
    TypedPipe.from(crossedPipe, ('t,'u))(tuple2Converter[T,U])
  }

  def flatMap[U](f: T => TraversableOnce[U]): TypedPipe[U] =
    new TypedPipe[U](inpipe, fields, { te => flatMapFn(te).flatMap(f) })

  /** limit the output to at most count items.
   * useful for debugging, but probably that's about it.
   * The number may be less than count, and not sampled particular method
   */
  def limit(count: Int): TypedPipe[T] =
    TypedPipe.from[T](pipe.limit(count), 0)

  // prints the current pipe to either stdout or stderr
  def debug: TypedPipe[T] =
    TypedPipe.from[T](this.pipe.debug, 0)

  /**
   * Returns the set of distinct elements in the TypedPipe
   */
  @annotation.implicitNotFound(msg = "For distinct method to work, the type in TypedPipe must have an Ordering.")
  def distinct(implicit ord: Ordering[_ >: T]): TypedPipe[T] = {
    // cast because Ordering is not contravariant, but should be (and this cast is safe)
    implicit val ordT: Ordering[T] = ord.asInstanceOf[Ordering[T]]
    map{ (_, ()) }.group.sum.keys
  }

  def map[U](f: T => U): TypedPipe[U] =
    new TypedPipe[U](inpipe, fields, { te => flatMapFn(te).map(f) })

  def mapValues[K, V, U](f : V => U)(implicit ev: T <:< (K, V)): TypedPipe[(K, U)] =
    map { t: T =>
      val (k, v) = t.asInstanceOf[(K, V)] //No need to capture ev and deal with serialization
      (k, f(v))
    }

  /** Keep only items satisfying a predicate
   */
  def filter(f: T => Boolean): TypedPipe[T] = {
    new TypedPipe[T](inpipe, fields, { te => flatMapFn(te).filter(f) })
  }
  /** flatten an Iterable */
  def flatten[U](implicit ev: T <:< TraversableOnce[U]): TypedPipe[U] =
    flatMap { _.asInstanceOf[TraversableOnce[U]] } // don't use ev which may not be serializable

  /** Force a materialization of this pipe prior to the next operation.
   * This is useful if you filter almost everything before a hashJoin, for instance.
   */
  lazy val forceToDisk: TypedPipe[T] = TypedPipe.from(pipe.forceToDisk, 0)(singleConverter[T])

  def group[K,V](implicit ev : <:<[T,(K,V)], ord : Ordering[K]) : Grouped[K,V] =
    //If the type of T is not (K,V), then at compile time, this will fail.  It uses implicits to do
    //a compile time check that one type is equivalent to another.  If T is not (K,V), we can't
    //automatically group.  We cast because it is safe to do so, and we need to convert to K,V, but
    //the ev is not needed for the cast.  In fact, you can do the cast with ev(t) and it will return
    //it as (K,V), but the problem is, ev is not serializable.  So we do the cast, which due to ev
    //being present, will always pass.
    groupBy { (t : T) => t.asInstanceOf[(K,V)]._1 }(ord)
      .mapValues { (t : T) => t.asInstanceOf[(K,V)]._2 }

  lazy val groupAll : Grouped[Unit,T] = groupBy(x => ()).withReducers(1)

  def groupBy[K](g : (T => K))(implicit ord : Ordering[K]) : Grouped[K,T] = {
    // due to type erasure, I'm fairly sure this is not using the primitive TupleGetters
    // Note, lazy val pipe returns a single count tuple with an object of type T in position 0
    val gpipe = pipe.mapTo[T,(K,T)](0 -> ('key, 'value)) { (t : T) => (g(t), t)}(singleConverter[T], TupleSetter.tup2Setter[(K,T)])
    Grouped.fromKVPipe[K,T](gpipe, ord)
  }
  def ++[U >: T](other : TypedPipe[U]) : TypedPipe[U] =
    TypedPipe.from(pipe ++ other.pipe, 0)(singleConverter[U])

  /** Reasonably common shortcut for cases of associative/commutative reduction
   * returns a typed pipe with only one element.
   */
  def sum[U >: T](implicit plus: Semigroup[U]): TypedPipe[U] = groupAll.sum[U].values

  def toPipe[U >: T](fieldNames: Fields)(implicit setter: TupleSetter[U]): Pipe =
    inpipe.flatMapTo[TupleEntry, U](fields -> fieldNames)(flatMapFn)

  def unpackToPipe[U >: T](fieldNames: Fields)(implicit up: TupleUnpacker[U]): Pipe = {
    val setter = up.newSetter(fieldNames)
    toPipe[U](fieldNames)(setter)
  }

  /** Safely write to a TypedSink[T]. If you want to write to a Source (not a Sink)
   * you need to do something like: toPipe(fieldNames).write(dest)
   * @return a pipe equivalent to the current pipe.
   */
  def write(dest: TypedSink[T])
    (implicit flowDef : FlowDef, mode : Mode): TypedPipe[T] = {

    val toWrite = pipe.mapTo[T,T](0 -> dest.sinkFields)(identity _)(singleConverter[T], dest.setter[T])
    dest.writeFrom(toWrite)
    // To avoid needing the inverse TupleConverter for dest.setter, just use the pipe before setting:
    TypedPipe.from[T](pipe, 0)
  }

  def keys[K](implicit ev : <:<[T,(K,_)]) : TypedPipe[K] = map { _._1 }

  // swap the keys with the values
  def swap[K,V](implicit ev: <:<[T,(K,V)]) : TypedPipe[(V,K)] = map { tup =>
    val (k,v) = tup.asInstanceOf[(K,V)]
    (v,k)
  }

  def values[V](implicit ev : <:<[T,(_,V)]) : TypedPipe[V] = map { _._2 }
}

@deprecated("Using Ordering.fromLessThan, duh..", "0.8.3")
class LtOrdering[T](ltfn : (T,T) => Boolean) extends Ordering[T] with Serializable {
  override def compare(left : T, right : T) : Int = {
    if(ltfn(left,right)) { -1 } else { if (ltfn(right, left)) 1 else 0 }
  }
  // This is faster than calling compare, which may result in two calls to ltfn
  override def lt(x : T, y : T) = ltfn(x,y)
}

class MappedOrdering[B,T](fn : (T) => B, ord : Ordering[B])
  extends Ordering[T] with Serializable {
  override def compare(left : T, right : T) : Int = ord.compare(fn(left), fn(right))
}

/** Represents sharded lists of items of type T
 */
trait KeyedList[+K,+T] {
  // These are the fundamental operations
  def toTypedPipe : TypedPipe[(K,T)]
  /** Operate on a Stream[T] of all the values for each key at one time.
   * Avoid accumulating the whole list in memory if you can.  Prefer reduce.
   */
  def mapValueStream[V](smfn : Iterator[T] => Iterator[V]) : KeyedList[K,V]

  ///////////
  /// The below are all implemented in terms of the above:
  ///////////

  /** Use Algebird Aggregator to do the reduction
   */
  def aggregate[B,C](agg: Aggregator[T,B,C]): TypedPipe[(K,C)] =
    mapValues(agg.prepare _)
      .reduce(agg.reduce _)
      .map { kv => (kv._1, agg.present(kv._2)) }

  /** This is a special case of mapValueStream, but can be optimized because it doesn't need
   * all the values for a given key at once.  An unoptimized implementation is:
   * mapValueStream { _.map { fn } }
   * but for Grouped we can avoid resorting to mapValueStream
   */
  def mapValues[V](fn : T => V) : KeyedList[K,V] = mapValueStream { _.map { fn } }
  /** reduce with fn which must be associative and commutative.
   * Like the above this can be optimized in some Grouped cases.
   * If you don't have a commutative operator, use reduceLeft
   */
  def reduce[U >: T](fn : (U,U) => U) : TypedPipe[(K,U)] = reduceLeft(fn)

  // The rest of these methods are derived from above
  def sum[U >: T](implicit sg: Semigroup[U]) = reduce(sg.plus)
  def product[U >: T](implicit ring : Ring[U]) = reduce(ring.times)
  def count(fn : T => Boolean) : TypedPipe[(K,Long)] = {
    mapValues { t => if (fn(t)) 1L else 0L }.sum
  }
  def forall(fn : T => Boolean) : TypedPipe[(K,Boolean)] = {
    mapValues { fn(_) }.product
  }

  /**
   * Selects all elements except first n ones.
   */
  def drop(n: Int) : KeyedList[K, T] = {
    mapValueStream { _.drop(n) }
  }

  /**
   * Drops longest prefix of elements that satisfy the given predicate.
   */
  def dropWhile(p: (T) => Boolean): KeyedList[K, T] = {
     mapValueStream {_.dropWhile(p)}
  }

  /**
   * Selects first n elements.
   */
  def take(n: Int) : KeyedList[K, T] = {
    mapValueStream {_.take(n)}
  }

  /**
   * Takes longest prefix of elements that satisfy the given predicate.
   */
  def takeWhile(p: (T) => Boolean) : KeyedList[K, T] = {
    mapValueStream {_.takeWhile(p)}
  }

  def foldLeft[B](z : B)(fn : (B,T) => B) : TypedPipe[(K,B)] = {
    mapValueStream { stream => Iterator(stream.foldLeft(z)(fn)) }
      .toTypedPipe
  }
  def scanLeft[B](z : B)(fn : (B,T) => B) : KeyedList[K,B] = {
    // Get the implicit conversion for scala 2.8 to have scanLeft on an iterator:
    import Dsl._
    mapValueStream { _.scanLeft(z)(fn) }
  }
  // Similar to reduce but always on the reduce-side (never optimized to mapside),
  // and named for the scala function. fn need not be associative and/or commutative.
  // Makes sense when you want to reduce, but in a particular sorted order.
  // the old value comes in on the left.
  def reduceLeft[U >: T](fn : (U,U) => U) : TypedPipe[(K,U)] = {
    mapValueStream[U] { stream =>
      if (stream.isEmpty) {
        // We have to guard this case, as cascading seems to give empty streams on occasions
        Iterator.empty
      }
      else {
        Iterator(stream.reduceLeft(fn))
      }
    }
    .toTypedPipe
  }
  def size : TypedPipe[(K,Long)] = mapValues { x => 1L }.sum
  def toList : TypedPipe[(K,List[T])] = mapValues { List(_) }.sum
  // Note that toSet needs to be parameterized even though toList does not.
  // This is because List is covariant in its type parameter in the scala API,
  // but Set is invariant.  See:
  // http://stackoverflow.com/questions/676615/why-is-scalas-immutable-set-not-covariant-in-its-type
  def toSet[U >: T] : TypedPipe[(K,Set[U])] = mapValues { Set[U](_) }.sum
  def max[B >: T](implicit cmp : Ordering[B]) : TypedPipe[(K,T)] = {
    asInstanceOf[KeyedList[K,B]].reduce(cmp.max).asInstanceOf[TypedPipe[(K,T)]]
  }
  def maxBy[B](fn : T => B)(implicit cmp : Ordering[B]) : TypedPipe[(K,T)] = {
    reduce((new MappedOrdering(fn, cmp)).max)
  }
  def min[B >: T](implicit cmp : Ordering[B]) : TypedPipe[(K,T)] = {
    asInstanceOf[KeyedList[K,B]].reduce(cmp.min).asInstanceOf[TypedPipe[(K,T)]]
  }
  def minBy[B](fn : T => B)(implicit cmp : Ordering[B]) : TypedPipe[(K,T)] = {
    reduce((new MappedOrdering(fn, cmp)).min)
  }
  def keys : TypedPipe[K] = toTypedPipe.keys
  def values : TypedPipe[T] = toTypedPipe.values
}

object Grouped {
  // Make a new Grouped from a pipe with two fields: 'key, 'value
  def fromKVPipe[K,V](pipe : Pipe, ordering : Ordering[K])
    (implicit conv : TupleConverter[V]) : Grouped[K,V] = {
    new Grouped[K,V](pipe, ordering, None, None, -1, false)
  }
  def valueSorting[T](implicit ord : Ordering[T]) : Fields = sorting("value", ord)

  def sorting[T](key : String, ord : Ordering[T]) : Fields = {
    val f = new Fields(key)
    f.setComparator(key, ord)
    f
  }
}
/** Represents a grouping which is the transition from map to reduce phase in hadoop.
 * Grouping is on a key of type K by ordering Ordering[K].
 */
class Grouped[K,+T] private (private[scalding] val pipe : Pipe,
  val ordering : Ordering[K],
  streamMapFn : Option[(Iterator[Tuple]) => Iterator[T]],
  private[scalding] val valueSort : Option[(Fields,Boolean)],
  val reducers : Int = -1,
  val toReducers: Boolean = false)
  extends KeyedList[K,T] with Serializable {

  import Dsl._
  private[scalding] val groupKey = Grouped.sorting("key", ordering)

  protected def sortIfNeeded(gb : GroupBuilder) : GroupBuilder = {
    valueSort.map { fb =>
      val gbSorted = gb.sortBy(fb._1)
      if (fb._2) gbSorted.reverse else gbSorted
    }.getOrElse(gb)
  }
  def forceToReducers: Grouped[K,T] =
    new Grouped(pipe, ordering, streamMapFn, valueSort, reducers, true)
  def withSortOrdering[U >: T](so : Ordering[U]) : Grouped[K,T] = {
    // Set the sorting with unreversed
    assert(valueSort.isEmpty, "Can only call withSortOrdering once")
    assert(streamMapFn.isEmpty, "Cannot sort after a mapValueStream")
    val newValueSort = Some(Grouped.valueSorting(so)).map { f => (f,false) }
    new Grouped(pipe, ordering, None, newValueSort, reducers, toReducers)
  }
  def withReducers(red : Int) : Grouped[K,T] = {
    new Grouped(pipe, ordering, streamMapFn, valueSort, red, toReducers)
  }
  def sortBy[B](fn : (T) => B)(implicit ord : Ordering[B]) : Grouped[K,T] = {
    withSortOrdering(new MappedOrdering(fn, ord))
  }
  // Sorts the values for each key
  def sorted[B >: T](implicit ord : Ordering[B]) : Grouped[K,T] = {
    // This cast is okay, because we are using the compare function
    // which is covariant, but the max/min functions are not, and that
    // breaks covariance.
    withSortOrdering(ord.asInstanceOf[Ordering[T]])
  }

  def sortWith(lt : (T,T) => Boolean) : Grouped[K,T] = {
    withSortOrdering(Ordering.fromLessThan(lt))
  }
  def reverse : Grouped[K,T] = {
    assert(streamMapFn.isEmpty, "Cannot reverse after mapValueStream")
    val newValueSort = valueSort.map { f => (f._1, !(f._2)) }
    new Grouped(pipe, ordering, None, newValueSort, reducers, toReducers)
  }

  protected def operate[T1](fn : GroupBuilder => GroupBuilder) : TypedPipe[(K,T1)] = {
    val reducedPipe = pipe.groupBy(groupKey) { gb =>
      val out = fn(sortIfNeeded(gb)).reducers(reducers)
      if(toReducers) out.forceToReducers else out
    }
    TypedPipe.from(reducedPipe, ('key, 'value))(tuple2Converter[K,T1])
  }
  // Here are the required KeyedList methods:
  override lazy val toTypedPipe : TypedPipe[(K,T)] = {
    if (streamMapFn.isEmpty && valueSort.isEmpty && (reducers == -1)) {
      // There was no reduce AND no mapValueStream, no need to groupBy:
      TypedPipe.from(pipe, ('key, 'value))(tuple2Converter[K,T])
    }
    else {
      //Actually execute the mapValueStream:
      streamMapFn.map { fn =>
        operate[T] {
          _.mapStream[Tuple,T]('value -> 'value)(fn)(CTupleConverter, singleSetter[T])
        }
      }.getOrElse {
        // This case happens when someone does .groupAll.sortBy { }.write
        // so there is no operation, they are just doing a sorted write
        operate[T] { identity _ }
      }
    }
  }
  override def mapValues[V](fn : T => V) : Grouped[K,V] = {
    if(valueSort.isEmpty && streamMapFn.isEmpty) {
      // We have no sort defined yet, so we should operate on the pipe so we can sort by V after
      // if we need to:
      new Grouped(pipe.map('value -> 'value)(fn)(singleConverter[T], singleSetter[V]),
        ordering, None, None, reducers, toReducers)
    }
    else {
      // There is a sorting, which invalidates map-side optimizations,
      // so we might as well use mapValueStream
      mapValueStream { iter => iter.map { fn } }
    }
  }
  // If there is no ordering, this operation is pushed map-side
  override def reduce[U >: T](fn : (U,U) => U) : TypedPipe[(K,U)] = {
    if(valueSort.isEmpty && streamMapFn.isEmpty) {
      // We can optimize mapside:
      val msr = new MapsideReduce(Semigroup.from(fn), 'key, 'value, None)(singleConverter[U], singleSetter[U])
      val mapSideReduced = pipe.eachTo(('key, 'value) -> ('key, 'value)) { _ => msr }
      // Now force to reduce-side for the rest, use groupKey to get the correct ordering
      val reducedPipe = mapSideReduced.groupBy(groupKey) {
        _.reduce('value -> 'value)(fn)(singleSetter[U], singleConverter[U])
          .reducers(reducers)
          .forceToReducers
      }
      TypedPipe.from(reducedPipe, ('key, 'value))(tuple2Converter[K,U])
    }
    else {
      // Just fall back to the mapValueStream based implementation:
      reduceLeft(fn)
    }
  }
  private[scalding] lazy val streamMapping : (Iterator[Tuple]) => Iterator[T] = {
    streamMapFn.getOrElse {
      // Set up the initial stream mapping:
      {(ti : Iterator[Tuple]) => ti.map { _.getObject(0).asInstanceOf[T] }}
    }
  }
  override def mapValueStream[V](nmf : Iterator[T] => Iterator[V]) : Grouped[K,V] = {
    val newStreamMapFn = Some(streamMapping.andThen(nmf))
    new Grouped[K,V](pipe, ordering, newStreamMapFn, valueSort, reducers, toReducers)
  }
  // SMALLER PIPE ALWAYS ON THE RIGHT!!!!!!!
  def cogroup[W,R](smaller: Grouped[K,W])(joiner: (K, Iterator[T], Iterable[W]) => Iterator[R])
    : KeyedList[K,R] = new CoGrouped2[K,T,W,R](this, smaller, joiner)

  def join[W](smaller : Grouped[K,W]) = cogroup(smaller)(Joiner.inner2)
  def leftJoin[W](smaller : Grouped[K,W]) = cogroup(smaller)(Joiner.left2)
  def rightJoin[W](smaller : Grouped[K,W]) = cogroup(smaller)(Joiner.right2)
  def outerJoin[W](smaller : Grouped[K,W]) = cogroup(smaller)(Joiner.outer2)

  /** WARNING This behaves semantically very differently than cogroup.
   * this is because we handle (K,T) tuples on the left as we see them.
   * the iterator on the right is over all elements with a matching key K, and it may be empty
   * if there are no values for this key K.
   * (because you haven't actually cogrouped, but only read the right hand side into a hashtable)
   */
  def hashCogroup[W,R](smaller: Grouped[K,W])(joiner: (K, T, Iterable[W]) => Iterator[R])
    : TypedPipe[(K,R)] = (new HashCoGrouped2[K,T,W,R](this, smaller, joiner)).toTypedPipe

  def hashJoin[W](smaller : Grouped[K,W]) : TypedPipe[(K,(T,W))] =
    hashCogroup(smaller)(Joiner.hashInner2)

  def hashLeftJoin[W](smaller : Grouped[K,W]) : TypedPipe[(K,(T,Option[W]))] =
    hashCogroup(smaller)(Joiner.hashLeft2)

  // TODO: implement blockJoin
}
