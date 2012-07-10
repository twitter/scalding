package com.twitter.scalding

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.tuple.Fields
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry

import java.io.Serializable

import com.twitter.scalding.mathematics.Monoid
import com.twitter.scalding.mathematics.Ring

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
object TDsl extends Serializable {
  //This can be used to avoid using groupBy:
  implicit def pipeToGrouped[K,V](tpipe : TypedPipe[(K,V)])(implicit ord : Ordering[K]) : Grouped[K,V] = {
    tpipe.group[K,V]
  }
  implicit def keyedToPipe[K,V](keyed : KeyedList[K,V]) : TypedPipe[(K,V)] = keyed.toTypedPipe
  implicit def pipeTExtensions(pipe : Pipe) : PipeTExtensions = new PipeTExtensions(pipe)
  implicit def mappableToTypedPipe[T](mappable : Mappable[T])
    (implicit flowDef : FlowDef, mode : Mode, conv : TupleConverter[T]) : TypedPipe[T] = {
    TypedPipe.from(mappable)(flowDef, mode, conv)
  }
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
  def from[T](pipe : Pipe, fields : Fields)(implicit conv : TupleConverter[T]) : TypedPipe[T] = {
    new TypedPipe[T](pipe, fields, {te => Some(conv(te))})
  }

  def from[T](mappable : Mappable[T])(implicit flowDef : FlowDef, mode : Mode, conv : TupleConverter[T]) = {
    new TypedPipe[T](mappable.read, mappable.sourceFields, {te => Some(conv(te))})
  }
}

/** Represents a phase in a distributed computation on an input data source
 * Wraps a cascading Pipe object, and holds the transformation done up until that point
 */
class TypedPipe[T](inpipe : Pipe, fields : Fields, flatMapFn : (TupleEntry) => Iterable[T])
  extends Serializable {
  import Dsl._

  /** This actually runs all the pure map functions in one Cascading Each
   * This approach is more efficient than untyped scalding because we
   * don't use TupleConverters/Setters after each map.
   * The output pipe has a single item CTuple with an object of type T in position 0
   */
  protected lazy val pipe : Pipe = {
    inpipe.flatMapTo(fields -> 0)(flatMapFn)(implicitly[TupleConverter[TupleEntry]], SingleSetter)
  }

  // Implements a cross project.  The right side should be tiny
  def cross[U](tiny : TypedPipe[U]) : TypedPipe[(T,U)] = {
    val crossedPipe = pipe.rename(0 -> 't)
      .crossWithTiny(tiny.pipe.rename(0 -> 'u))
    TypedPipe.from(crossedPipe, ('t,'u))(implicitly[TupleConverter[(T,U)]])
  }

  def flatMap[U](f : T => Iterable[U]) : TypedPipe[U] = {
    new TypedPipe[U](inpipe, fields, { te => flatMapFn(te).flatMap(f) })
  }
  def map[U](f : T => U) : TypedPipe[U] = {
    new TypedPipe[U](inpipe, fields, { te => flatMapFn(te).map(f) })
  }
  def filter( f : T => Boolean) : TypedPipe[T] = {
    new TypedPipe[T](inpipe, fields, { te => flatMapFn(te).filter(f) })
  }
  def group[K,V](implicit ev : =:=[T,(K,V)], ord : Ordering[K]) : Grouped[K,V] = {

    //If the type of T is not (K,V), then at compile time, this will fail.  It uses implicits to do
    //a compile time check that one type is equivalent to another.  If T is not (K,V), we can't
    //automatically group.  We cast because it is safe to do so, and we need to convert to K,V, but
    //the ev is not needed for the cast.  In fact, you can do the cast with ev(t) and it will return
    //it as (K,V), but the problem is, ev is not serializable.  So we do the cast, which due to ev
    //being present, will always pass.

    groupBy { (t : T) => t.asInstanceOf[(K,V)]._1 }(ord)
      .mapValues { (t : T) => t.asInstanceOf[(K,V)]._2 }
  }

  def groupAll : Grouped[Unit,T] = groupBy(x => ()).withReducers(1)

  def groupBy[K](g : (T => K))(implicit  ord : Ordering[K]) : Grouped[K,T] = {
    // TODO due to type erasure, I'm fairly sure this is not using the primitive TupleGetters
    // Note, lazy val pipe returns a single count tuple with an object of type T in position 0
    val gpipe = pipe.mapTo(0 -> ('key, 'value)) { (t : T) => (g(t), t)}
    Grouped.fromKVPipe(gpipe, ord)
  }
  def ++[U >: T](other : TypedPipe[U]) : TypedPipe[U] = {
    TypedPipe.from(pipe ++ other.pipe, 0)(singleConverter[U])
  }

  def toPipe(fieldNames : Fields)(implicit setter : TupleSetter[T]) : Pipe = {
    val conv = implicitly[TupleConverter[TupleEntry]]
    inpipe.flatMapTo(fields -> fieldNames)(flatMapFn)(conv, setter)
  }
  def unpackToPipe(fieldNames : Fields)(implicit up : TupleUnpacker[T]) : Pipe = {
    val setter = up.newSetter(fieldNames)
    toPipe(fieldNames)(setter)
  }

  /** A convenience method equivalent to toPipe(fieldNames).write(dest)
   * @return a pipe equivalent to the current pipe.
   */
  def write(fieldNames : Fields, dest : Source)
    (implicit conv : TupleConverter[T], setter : TupleSetter[T], flowDef : FlowDef, mode : Mode) : TypedPipe[T] = {
    val pipe = toPipe(fieldNames)(setter)
    pipe.write(dest)
    // Now, we have written out, so let's start from here with the new pipe:
    // If we don't do this, Cascading's flow planner can't see what's happening
    TypedPipe.from(pipe, fieldNames)(conv)
  }
}

class LtOrdering[T](lt : (T,T) => Boolean) extends Ordering[T] with Serializable {
  override def compare(left : T, right : T) : Int = {
    if(lt(left,right)) { -1 } else { if (lt(right, left)) 1 else 0 }
  }
}

class MappedOrdering[B,T](fn : (T) => B, ord : Ordering[B])
  extends Ordering[T] with Serializable {
  override def compare(left : T, right : T) : Int = ord.compare(fn(left), fn(right))
}

/** Represents sharded lists of items of type T
 */
trait KeyedList[K,T] {
  // These are the fundamental operations
  def toTypedPipe : TypedPipe[(K,T)]
  /** Operate on a Stream[T] of all the values for each key at one time.
   * Avoid accumulating the whole list in memory if you can.  Prefer reduce.
   */
  def mapValueStream[V](smfn : Iterator[T] => Iterator[V]) : KeyedList[K,V]
  /** This is a special case of mapValueStream, but can be optimized because it doesn't need
   * all the values for a given key at once.  An unoptimized implementation would be:
   * mapValueStream { _.map { fn } }
   * but for Grouped we can avoid resorting to mapValueStream
   */
  def mapValues[V](fn : T => V) : KeyedList[K,V]
  /** reduce with fn which must be associative and commutative.
   * Like the above this can be optimized in some Grouped cases.
   */
  def reduce(fn : (T,T) => T) : TypedPipe[(K,T)] = reduceLeft(fn)

  // The rest of these methods are derived from above
  def sum(implicit monoid : Monoid[T]) = reduce(monoid.plus)
  def product(implicit ring : Ring[T]) = reduce(ring.times)
  def count(fn : T => Boolean) : TypedPipe[(K,Long)] = {
    mapValues { t => if (fn(t)) 1L else 0L }.sum
  }
  def forall(fn : T => Boolean) : TypedPipe[(K,Boolean)] = {
    mapValues { fn(_) }.product
  }
  def foldLeft[B](z : B)(fn : (B,T) => B) : TypedPipe[(K,B)] = {
    mapValueStream { stream => Some(stream.foldLeft(z)(fn)).toIterator }
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
  def reduceLeft( fn : (T,T) => T) : TypedPipe[(K,T)] = {
    mapValueStream[T] { stream =>
      if (stream.isEmpty) {
        // We have to guard this case, as cascading seems to give empty streams on occasions
        None.iterator
      }
      else {
        Some(stream.reduceLeft(fn)).iterator
      }
    }
    .toTypedPipe
  }
  def size : TypedPipe[(K,Long)] = mapValues { x => 1L }.sum
  def toList : TypedPipe[(K,List[T])] = mapValues { List(_) }.sum
  def toSet : TypedPipe[(K,Set[T])] = mapValues { Set(_) }.sum
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
}

object Grouped {
  // Make a new Grouped from a pipe with two fields: 'key, 'value
  def fromKVPipe[K,V](pipe : Pipe, ordering : Ordering[K])
    (implicit conv : TupleConverter[V]) : Grouped[K,V] = {
    new Grouped[K,V](pipe, ordering, None, None, -1)
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
class Grouped[K,T](val pipe : Pipe, ordering : Ordering[K],
  streamMapFn : Option[(Iterator[TupleEntry]) => Iterator[T]],
  valueSort : Option[(Fields,Boolean)],
  reducers : Int = -1)
  extends KeyedList[K,T] with Serializable {

  import Dsl._
  protected val groupKey = Grouped.sorting("key", ordering)

  protected def sortIfNeeded(gb : GroupBuilder) : GroupBuilder = {
    valueSort.map { fb =>
      val gbSorted = gb.sortBy(fb._1)
      if (fb._2) gbSorted.reverse else gbSorted
    }.getOrElse(gb)
  }
  def withSortOrdering(so : Ordering[T]) : Grouped[K,T] = {
    // Set the sorting with unreversed
    assert(valueSort.isEmpty, "Can only call withSortOrdering once")
    assert(streamMapFn.isEmpty, "Cannot sort after a mapValueStream")
    val newValueSort = Some(Grouped.valueSorting(so)).map { f => (f,false) }
    new Grouped(pipe, ordering, None, newValueSort, reducers)
  }
  def withReducers(red : Int) : Grouped[K,T] = {
    new Grouped(pipe, ordering, streamMapFn, valueSort, red)
  }
  def sortBy[B](fn : (T) => B)(implicit ord : Ordering[B]) : Grouped[K,T] = {
    withSortOrdering(new MappedOrdering(fn, ord))
  }
  def sortWith(lt : (T,T) => Boolean) : Grouped[K,T] = {
    withSortOrdering(new LtOrdering(lt))
  }
  def reverse : Grouped[K,T] = {
    assert(streamMapFn.isEmpty, "Cannot reverse after mapValueStream")
    val newValueSort = valueSort.map { f => (f._1, !(f._2)) }
    new Grouped(pipe, ordering, None, newValueSort, reducers)
  }

  protected def operate[T1](fn : GroupBuilder => GroupBuilder) : TypedPipe[(K,T1)] = {
    val reducedPipe = pipe.groupBy(groupKey) { gb =>
      fn(sortIfNeeded(gb)).reducers(reducers)
    }
    TypedPipe.from(reducedPipe, ('key, 'value))(implicitly[TupleConverter[(K,T1)]])
  }
  // Here are the required KeyedList methods:
  override lazy val toTypedPipe : TypedPipe[(K,T)] = {
    if (streamMapFn.isEmpty) {
      // There was no reduce AND no mapValueStream, no need to groupBy:
      TypedPipe.from(pipe, ('key, 'value))(implicitly[TupleConverter[(K,T)]])
    }
    else {
      //Actually execute the mapValueStream:
      operate[T] { _.mapStream[TupleEntry,T]('value -> 'value)(streamMapFn.get)(TupleEntryConverter,SingleSetter)
      }
    }
  }
  override def mapValues[V](fn : T => V) : Grouped[K,V] = {
    if(valueSort.isEmpty && streamMapFn.isEmpty) {
      // We have no sort defined yet, so we should operate on the pipe so we can sort by V after
      // if we need to:
      new Grouped(pipe.map('value -> 'value)(fn)(singleConverter[T], SingleSetter),
        ordering, None, None, reducers)
    }
    else {
      // There is a sorting, which invalidates map-side optimizations,
      // so we might as well use mapValueStream
      mapValueStream { iter => iter.map { fn } }
    }
  }
  // If there is no ordering, this operation is pushed map-side
  override def reduce(fn : (T,T) => T) : TypedPipe[(K,T)] = {
    if(valueSort.isEmpty && streamMapFn.isEmpty) {
      // We can optimize mapside:
      operate[T] { _.reduce[T]('value -> 'value)(fn)(SingleSetter, singleConverter[T]) }
    }
    else {
      // Just fall back to the mapValueStream based implementation:
      reduceLeft(fn)
    }
  }
  override def mapValueStream[V](nmf : Iterator[T] => Iterator[V]) : Grouped[K,V] = {
    val tconv = singleConverter[T]
    val newStreamMapFn = streamMapFn.map { _.andThen(nmf) }.orElse {
      // Set up the initial stream mapping:
      Some({(tei : Iterator[TupleEntry]) => nmf(tei.map { te => tconv(te) })})
    }
    new Grouped[K,V](pipe, ordering, newStreamMapFn, valueSort, reducers)
  }
  // SMALLER PIPE ALWAYS ON THE RIGHT!!!!!!!
  def join[W](smaller : Grouped[K,W]) = new InnerCoGrouped2[K,T,W](this, smaller)
  def leftJoin[W](smaller : Grouped[K,W]) = new LeftCoGrouped2[K,T,W](this, smaller)
  def rightJoin[W](smaller : Grouped[K,W]) = new RightCoGrouped2[K,T,W](this, smaller)
  def outerJoin[W](smaller : Grouped[K,W]) = new OuterCoGrouped2[K,T,W](this, smaller)
  // TODO: implement blockJoin, joinWithTiny
}


/** Represents a result of CoGroup operation on two Grouped pipes.
 * users should probably never directly construct them, but instead use the
 * (outer/left/right)Join methods of Grouped.
 */
class CoGrouped2[K,V,W,Result]
  (bigger : Grouped[K,V], bigMode : JoinMode, smaller : Grouped[K,W], smallMode : JoinMode,
  conv : ((V,W)) => Result, streamMapFn : Option[(Iterator[(V,W)]) => Iterator[Result]],
  reducers : Int = -1)
  extends KeyedList[K,Result] with Serializable {

  import Dsl._

  protected def nonNullKey(tup : Tuple) : K = {
    val first = tup.getObject(0)
    val second = tup.getObject(1)
    val idx = if(null == first) 1 else 0
    // TODO, POB: I think type erasure is making this TupleGetter[AnyRef]
    // And so, some of this *might* break if Cascading starts handling primitives
    // better
    implicitly[TupleGetter[K]].get(tup, idx)
  }

  // resultFields should have the two key fields first
  protected def operate[B](op : CoGroupBuilder => GroupBuilder,
    resultFields : Fields, finish : Tuple => B) : TypedPipe[(K,B)] = {
    // Rename the key and values:
    val rsmaller = smaller.pipe.rename(('key, 'value) -> ('key2, 'value2))
    val newPipe = bigger.pipe.coGroupBy('key, bigMode) { gb =>
      op(gb.coGroup('key2, rsmaller, smallMode)).reducers(reducers)
    }.project(resultFields)
    new TypedPipe[(K,B)](newPipe, resultFields, { te =>
      val tup = te.getTuple
      Some(nonNullKey(tup), finish(tup))
    })
  }
  // If you don't reduce, this should be an implicit CoGrouped => TypedPipe
  override lazy val toTypedPipe : TypedPipe[(K,Result)] = {
    if (streamMapFn.isEmpty) {
      // This is the case of no reduce or mapValueStream after the join (common case)
      operate({ gb => gb },
        ('key, 'key2, 'value, 'value2),
        {tup : Tuple => conv((tup.getObject(2).asInstanceOf[V], tup.getObject(3).asInstanceOf[W]))})
    }
    else {
      // We do the mapStream.  NOTE conv must be null if we get here
      assert(conv == null, "Cannot have non-null conv and streamMapFn")
      operate({gb => gb.mapStream(('value,'value2)->('valuer))(streamMapFn.get)},
      ('key, 'key2, 'valuer),
      {tup : Tuple => tup.getObject(2).asInstanceOf[Result]})
    }
  }
  override def mapValues[B]( f : (Result) => B) : KeyedList[K,B] = {
    if (streamMapFn.isEmpty) {
      // We can just chain the conv function from (V,W) => Result => B
      new CoGrouped2[K,V,W,B](bigger, bigMode, smaller, smallMode,
        conv.andThen(f), None, reducers)
    }
    else {
      // we might as well use mapValueStream
      mapValueStream { iter => iter.map { f } }
    }
  }
  override def mapValueStream[U](f : Iterator[Result] => Iterator[U]) : KeyedList[K,U] = {
    val newStreamMapFn = streamMapFn.map {
        // If it is defined, just chain in f after what we already have
        _.andThen { f }
      }
      .orElse {
        // This the first mapValueStream call, need to setup initial streamMapFn:
        Some( (vwit : Iterator[(V,W)]) => f(vwit.map { conv }) )
      }
    new CoGrouped2[K,V,W,U](bigger, bigMode, smaller, smallMode,
      null, newStreamMapFn, reducers)
  }

  def withReducers(red : Int) : CoGrouped2[K,V,W,Result] = {
    new CoGrouped2(bigger, bigMode, smaller, smallMode, conv, streamMapFn, red)
  }
}

class InnerCoGrouped2[K,V,W](bigger : Grouped[K,V], smaller : Grouped[K,W])
  extends CoGrouped2[K,V,W,(V,W)](bigger, InnerJoinMode, smaller, InnerJoinMode,
    { in : (V,W) => in }, None)

class LeftCoGrouped2[K,V,W](bigger : Grouped[K,V], smaller : Grouped[K,W])
  extends CoGrouped2[K,V,W,(V,Option[W])](bigger, InnerJoinMode, smaller, OuterJoinMode,
    { in : (V,W) => (in._1, Option(in._2))}, None)

class RightCoGrouped2[K,V,W](bigger : Grouped[K,V], smaller : Grouped[K,W])
  extends CoGrouped2[K,V,W,(Option[V],W)](bigger, OuterJoinMode, smaller, InnerJoinMode,
    { in : (V,W) => (Option(in._1), in._2)}, None)

class OuterCoGrouped2[K,V,W](bigger : Grouped[K,V], smaller : Grouped[K,W])
  extends CoGrouped2[K,V,W,(Option[V],Option[W])](bigger, OuterJoinMode, smaller, OuterJoinMode,
    { in : (V,W) => (Option(in._1), Option(in._2))}, None)
