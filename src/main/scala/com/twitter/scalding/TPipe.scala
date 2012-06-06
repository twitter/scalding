package com.twitter.scalding

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.tuple.Fields
import cascading.tuple.Tuple

import java.io.Serializable

import com.twitter.scalding.mathematics.Monoid
import com.twitter.scalding.mathematics.Ring

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

  //This can be used to avoid using groupBy:
  implicit def pipeToGrouped[K,V](tpipe : TPipe[(K,V)])(implicit ord : Ordering[K]) : Grouped[K,V] = {
    tpipe.group[K,V]
  }
}

class TPipe[T](protected val pipe : Pipe) {
  import Dsl._
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

class Grouped[K,T](pipe : Pipe, ordering : Ordering[K], sortfn : Option[Ordering[T]] = None) {

  import Dsl._
  protected class LtOrdering[T](lt : (T,T) => Boolean) extends Ordering[T] with Serializable {
    override def compare(left : T, right : T) : Int = {
      if(lt(left,right)) { -1 } else { if (lt(right, left)) 1 else 0 }
    }
  }
  protected class MappedOrdering[B,T](fn : (T) => B, ord : Ordering[B])
    extends Ordering[T] with Serializable {
    override def compare(left : T, right : T) : Int = ord.compare(fn(left), fn(right))
  }
  protected val groupKey = {
    val f = new Fields("key")
    f.setComparator("key", ordering)
    f
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

  // Ignores any ordering, must be commutative and associative
  def reduce(fn : (T,T) => T) : TPipe[(K,T)] = {
    val reducedPipe = pipe.groupBy(groupKey) {
      _.reduce[T]('value -> 'value)(fn)(SingleSetter, singleConverter[T])
    }.mapTo(('key, 'value) -> 0)({tup : Tuple =>
      (tup.getObject(0).asInstanceOf[K], tup.getObject(1).asInstanceOf[T])
    })(implicitly[TupleConverter[Tuple]], SingleSetter)
    new TPipe[(K,T)](reducedPipe)
  }
  // Some combinable functions:
  def count(fn : T => Boolean) : TPipe[(K,Long)] = {
    mapValues { t => if (fn(t)) 1L else 0L }.sum
  }
  def sum(implicit monoid : Monoid[T]) = reduce(monoid.plus)
  def product(implicit ring : Ring[T]) = reduce(ring.times)
  def max[B >: T](implicit cmp : Ordering[B]) : TPipe[(K,T)] = {
    asInstanceOf[Grouped[K,B]].reduce(cmp.max).asInstanceOf[TPipe[(K,T)]]
  }
  def maxBy[B](fn : T => B)(implicit cmp : Ordering[B]) : TPipe[(K,T)] = {
    reduce((new MappedOrdering(fn, cmp)).max)
  }
  def min[B >: T](implicit cmp : Ordering[B]) : TPipe[(K,T)] = {
    asInstanceOf[Grouped[K,B]].reduce(cmp.min).asInstanceOf[TPipe[(K,T)]]
  }
  def minBy[B](fn : T => B)(implicit cmp : Ordering[B]) : TPipe[(K,T)] = {
    reduce((new MappedOrdering(fn, cmp)).min)
  }

  // Ordered traversal of the data
  def foldLeft[B](z : B)(fn : (B,T) => B) : TPipe[(K,B)] = {
    val reducedPipe = pipe.groupBy(groupKey) { gb =>
      sortfn.map { cmp =>
        val f = new Fields("value")
        f.setComparator("value", cmp)
        gb.sortBy(f)
      }.getOrElse(gb)
        .foldLeft[B,T]('value -> 'value)(z)(fn)(SingleSetter, singleConverter[T])
    }.mapTo(('key, 'value) -> 0)({tup : Tuple =>
      (tup.getObject(0).asInstanceOf[K], tup.getObject(1).asInstanceOf[T])
    })(implicitly[TupleConverter[Tuple]], SingleSetter)
    new TPipe[(K,B)](reducedPipe)
  }

  def scanLeft[B](z : B)(fn : (B,T) => B) : TPipe[(K,B)] = {
    val reducedPipe = pipe.groupBy(groupKey) { gb =>
      sortfn.map { cmp =>
        val f = new Fields("value")
        f.setComparator("value", cmp)
        gb.sortBy(f)
      }.getOrElse(gb)
        .scanLeft[B,T]('value -> 'value)(z)(fn)(SingleSetter, singleConverter[T])
    }.mapTo(('key, 'value) -> 0)({tup : Tuple =>
      (tup.getObject(0).asInstanceOf[K], tup.getObject(1).asInstanceOf[T])
    })(implicitly[TupleConverter[Tuple]], SingleSetter)
    new TPipe[(K,B)](reducedPipe)
  }
}
/*
  //CoGrouping/Joining
  // Left join:
  def |*[W](that : Grouped[K,W]) : CoGrouped2[K,V,Option[W]]
  // Right Join
  def *|[W](that : Grouped[K,W]) : CoGrouped2[K,Option[V],W]
  // Inner Join
  def *[W](that : Grouped[K,W]) : CoGrouped2[K,V,W]
  // Outer Join
  def |*|[W](that : Grouped[K,W]) : CoGrouped2[K,Option[V],Option[W]]
}

class CoGrouped2[K,V,W] {
  // If you don't reduce, this should be an implicit CoGrouped => TPipe
  def toTPipe : TPipe[(K,(V,W))]
  def foldLeft[B](z : B)(f : (B,(V,W)) => B) : TPipe[(K,B)]
  def scanLeft[B](z : B)(f : (B,(V,W)) => B) : TPipe[(K,B)]
}
*/
