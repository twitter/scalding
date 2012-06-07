package com.twitter.scalding

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.tuple.Fields
import cascading.tuple.Tuple

import java.io.Serializable

import com.twitter.scalding.mathematics.Monoid
import com.twitter.scalding.mathematics.Ring

object TDsl {
  //This can be used to avoid using groupBy:
  implicit def pipeToGrouped[K,V](tpipe : TPipe[(K,V)])(implicit ord : Ordering[K]) : Grouped[K,V] = {
    tpipe.group[K,V]
  }
  implicit def keyedToPipe[K,V](keyed : KeyedList[K,V]) : TPipe[(K,V)] = keyed.toTPipe
}

object TPipe {
  // Get the implicit field/tuple conversions
  import Dsl._

  def from[T](pipe : Pipe, fields : Fields)(implicit conv : TupleConverter[T]) : TPipe[T] = {
    conv.assertArityMatches(fields)
    //Just stuff it in a single element cascading Tuple:
    val fn : T => T = identity _
    new TPipe[T](pipe.mapTo(fields -> 0)(fn)(conv,SingleSetter))
  }

  def from[T](mappable : Mappable[T])(implicit flowDef : FlowDef, mode : Mode, conv : TupleConverter[T]) = {
    val fn : T => T = identity _
    new TPipe[T](mappable.mapTo(0)(fn)(flowDef, mode, conv, SingleSetter))
  }
}

class TPipe[T](protected val pipe : Pipe) {
  import Dsl._

  // Implements a cross project.  The right side should be tiny
  def cross[U](tiny : TPipe[U]) : TPipe[(T,U)] = {
    val crossedPipe = pipe.rename(0 -> 't)
      .crossWithTiny(tiny.pipe.rename(0 -> 'u))
      // Now put them into a single tuple:
      .mapTo[(T,U),(T,U)](('t,'u) -> 0)(identity _)(implicitly[TupleConverter[(T,U)]], SingleSetter)
    new TPipe[(T,U)](crossedPipe)
  }

  def flatMap[U](f : T => Iterable[U]) : TPipe[U] = {
    new TPipe[U](pipe.flatMapTo(0 -> 0)(f)(singleConverter[T], SingleSetter))
  }
  def map[U](f : T => U) : TPipe[U] = {
    new TPipe[U](pipe.mapTo(0 -> 0)(f)(singleConverter[T], SingleSetter))
  }
  def filter( f : T => Boolean) : TPipe[T] = {
    new TPipe[T](pipe.filter(0)(f)(singleConverter[T]))
  }
  def group[K,V](implicit ev : =:=[T,(K,V)], ord : Ordering[K]) : Grouped[K,V] = {
    groupBy { (t : T) => ev(t)._1 }(ord).mapValues { (t : T) => ev(t)._2 }
  }
  def groupBy[K](g : (T => K))(implicit ord : Ordering[K]) : Grouped[K,T] = {
    val gpipe = pipe.mapTo(0 -> ('key, 'value))({ t : T => (g(t),t)})(singleConverter[T], Tup2Setter)
    new Grouped[K,T](gpipe, ord, None)
  }
  def ++[U >: T](other : TPipe[U]) : TPipe[U] = new TPipe[U](pipe ++ other.pipe)

  def toPipe(fieldNames : Fields)(implicit setter : TupleSetter[T]) : Pipe = {
    pipe.mapTo[T,T](0 -> fieldNames)(identity _)(singleConverter[T], setter)
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
  def toTPipe : TPipe[(K,T)]
  def mapValues[V](fn : T => V) : KeyedList[K,V]
  def reduce(fn : (T,T) => T) : TPipe[(K,T)]
  def foldLeft[B](z : B)(fn : (B,T) => B) : TPipe[(K,B)]
  def scanLeft[B](z : B)(fn : (B,T) => B) : TPipe[(K,B)]

  //These we derive from the above:
  def count(fn : T => Boolean) : TPipe[(K,Long)] = {
    mapValues { t => if (fn(t)) 1L else 0L }.sum
  }
  def sum(implicit monoid : Monoid[T]) = reduce(monoid.plus)
  def toList : TPipe[(K,List[T])] = mapValues { List(_) }.sum
  def toSet : TPipe[(K,Set[T])] = mapValues { Set(_) }.sum
  def product(implicit ring : Ring[T]) = reduce(ring.times)
  def max[B >: T](implicit cmp : Ordering[B]) : TPipe[(K,T)] = {
    asInstanceOf[KeyedList[K,B]].reduce(cmp.max).asInstanceOf[TPipe[(K,T)]]
  }
  def maxBy[B](fn : T => B)(implicit cmp : Ordering[B]) : TPipe[(K,T)] = {
    reduce((new MappedOrdering(fn, cmp)).max)
  }
  def min[B >: T](implicit cmp : Ordering[B]) : TPipe[(K,T)] = {
    asInstanceOf[KeyedList[K,B]].reduce(cmp.min).asInstanceOf[TPipe[(K,T)]]
  }
  def minBy[B](fn : T => B)(implicit cmp : Ordering[B]) : TPipe[(K,T)] = {
    reduce((new MappedOrdering(fn, cmp)).min)
  }
}

class Grouped[K,T](val pipe : Pipe, ordering : Ordering[K], sortfn : Option[Ordering[T]] = None) extends KeyedList[K,T] {

  import Dsl._
  protected val groupKey = {
    val f = new Fields("key")
    f.setComparator("key", ordering)
    f
  }
  protected def sortIfNeeded(gb : GroupBuilder) : GroupBuilder = {
    sortfn.map { cmp =>
      val f = new Fields("value")
      f.setComparator("value", cmp)
      gb.sortBy(f)
    }.getOrElse(gb)
  }
  // Here only for KeyedList, probably never useful
  def toTPipe : TPipe[(K,T)] = {
    new TPipe[(K,T)](pipe.mapTo(('key, 'value) -> 0)({tup : Tuple =>
      (tup.getObject(0).asInstanceOf[K], tup.getObject(1).asInstanceOf[T])
    })(implicitly[TupleConverter[Tuple]], SingleSetter))
  }
  def mapValues[V](fn : T => V) : Grouped[K,V] = {
    new Grouped(pipe.map('value -> 'value)(fn)(singleConverter[T], SingleSetter), ordering)
  }
  def withSortOrdering(so : Ordering[T]) : Grouped[K,T] = {
    new Grouped[K,T](pipe, ordering, Some(so))
  }
  def sortBy[B](fn : (T) => B)(implicit ord : Ordering[B]) : Grouped[K,T] = {
    withSortOrdering(new MappedOrdering(fn, ord))
  }
  def sortWith(lt : (T,T) => Boolean) : Grouped[K,T] = {
    withSortOrdering(new LtOrdering(lt))
  }
  def reverse : Grouped[K,T] = new Grouped(pipe, ordering, sortfn.map { _.reverse })

  protected def operate[T1](fn : GroupBuilder => GroupBuilder) : TPipe[(K,T1)] = {
    val reducedPipe = pipe.groupBy(groupKey) { gb =>
      fn(sortIfNeeded(gb))
    }.mapTo(('key, 'value) -> 0)({tup : Tuple =>
      (tup.getObject(0).asInstanceOf[K], tup.getObject(1).asInstanceOf[T1])
    })(implicitly[TupleConverter[Tuple]], SingleSetter)
    new TPipe[(K,T1)](reducedPipe)
  }

  // If there is no ordering, this operation is pushed map-side
  def reduce(fn : (T,T) => T) : TPipe[(K,T)] = {
    operate[T] { _.reduce[T]('value -> 'value)(fn)(SingleSetter, singleConverter[T]) }
  }
  // Ordered traversal of the data
  def foldLeft[B](z : B)(fn : (B,T) => B) : TPipe[(K,B)] = {
    operate[B] { _.foldLeft[B,T]('value -> 'value)(z)(fn)(SingleSetter, singleConverter[T]) }
  }

  def scanLeft[B](z : B)(fn : (B,T) => B) : TPipe[(K,B)] = {
    operate[B] { _.scanLeft[B,T]('value -> 'value)(z)(fn)(SingleSetter, singleConverter[T]) }
  }
  // SMALLER PIPE ALWAYS ON THE RIGHT!!!!!!!
  def join[W](smaller : Grouped[K,W]) = new InnerCoGrouped2[K,T,W](this, smaller)
  def leftJoin[W](smaller : Grouped[K,W]) = new LeftCoGrouped2[K,T,W](this, smaller)
  def rightJoin[W](smaller : Grouped[K,W]) = new RightCoGrouped2[K,T,W](this, smaller)
  def outerJoin[W](smaller : Grouped[K,W]) = new OuterCoGrouped2[K,T,W](this, smaller)
  // TODO: implement blockJoin
}

class CoGrouped2[K,V,W,Result]
  (bigger : Grouped[K,V], bigJoiner : JoinMode, smaller : Grouped[K,W], smallMode : JoinMode,
  conv : ((V,W)) => Result)
  extends KeyedList[K,Result] with Serializable {

  import Dsl._

  def nonNullOf(first : K, second : K) : K = if(null == first) second else first

  // resultFields should have the two key fields first
  protected def operate[B](op : CoGroupBuilder => GroupBuilder,
    resultFields : Fields, finish : Tuple => B) : TPipe[(K,B)] = {
    // Rename the key and values:
    val rsmaller = smaller.pipe.rename(('key, 'value) -> ('key2, 'value2))
    new TPipe[(K,B)](bigger.pipe.coGroupBy('key, bigJoiner) { gb =>
      op(gb.coGroup('key2, rsmaller, smallMode))
    }
    // Now get the pipe into the right format:
    .mapTo(resultFields -> 0)({tup : Tuple =>
      val k = tup.getObject(0).asInstanceOf[K]
      val k2 = tup.getObject(1).asInstanceOf[K]
      (nonNullOf(k, k2), finish(tup))
    })(implicitly[TupleConverter[Tuple]], SingleSetter))
  }
  // If you don't reduce, this should be an implicit CoGrouped => TPipe
  def toTPipe : TPipe[(K,Result)] = {
    operate({ gb => gb },
      ('key, 'key2, 'value, 'value2),
      {tup : Tuple => conv((tup.getObject(2).asInstanceOf[V], tup.getObject(3).asInstanceOf[W]))})
  }
  def mapValues[B]( f : (Result) => B) : KeyedList[K,B] = {
    new CoGrouped2[K,V,W,B](bigger, bigJoiner, smaller, smallMode, conv.andThen(f))
  }
  override def reduce(f : (Result,Result) => Result) : TPipe[(K,Result)] = {
    operate({ gb =>
        gb.mapReduceMap(('value, 'value2) -> ('result))(conv)(f)(identity
        _)(implicitly[TupleConverter[(V,W)]], SingleSetter, singleConverter[Result],SingleSetter)
      },
      ('key, 'key2, 'result),
      {(tup : Tuple) => tup.getObject(2).asInstanceOf[Result]})
  }
  def foldLeft[B](z : B)(f : (B,Result) => B) : TPipe[(K,B)] = {
    def newFoldFn(old : B, data : (V,W)) : B = f(old, conv(data))
    operate({gb => gb.foldLeft(('value,'value2)->('valueb))(z)(newFoldFn _)},
      ('key, 'key2, 'valueb),
      {tup : Tuple => tup.getObject(2).asInstanceOf[B]})
  }
  def scanLeft[B](z : B)(f : (B,Result) => B) : TPipe[(K,B)] = {
    def newFoldFn(old : B, data : (V,W)) : B = f(old, conv(data))
    operate({gb => gb.scanLeft(('value,'value2)->('valueb))(z)(newFoldFn _)},
      ('key, 'key2, 'valueb),
      {tup : Tuple => tup.getObject(2).asInstanceOf[B]})
  }
}

class InnerCoGrouped2[K,V,W](bigger : Grouped[K,V], smaller : Grouped[K,W])
  extends CoGrouped2[K,V,W,(V,W)](bigger, InnerJoinMode, smaller, InnerJoinMode,
    { in : (V,W) => in })

class LeftCoGrouped2[K,V,W](bigger : Grouped[K,V], smaller : Grouped[K,W])
  extends CoGrouped2[K,V,W,(V,Option[W])](bigger, InnerJoinMode, smaller, OuterJoinMode,
    { in : (V,W) => (in._1, Option(in._2))})

class RightCoGrouped2[K,V,W](bigger : Grouped[K,V], smaller : Grouped[K,W])
  extends CoGrouped2[K,V,W,(Option[V],W)](bigger, OuterJoinMode, smaller, InnerJoinMode,
    { in : (V,W) => (Option(in._1), in._2)})

class OuterCoGrouped2[K,V,W](bigger : Grouped[K,V], smaller : Grouped[K,W])
  extends CoGrouped2[K,V,W,(Option[V],Option[W])](bigger, OuterJoinMode, smaller, OuterJoinMode,
    { in : (V,W) => (Option(in._1), Option(in._2))})
