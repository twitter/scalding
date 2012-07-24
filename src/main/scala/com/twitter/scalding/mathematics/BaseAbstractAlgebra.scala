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
package com.twitter.scalding.mathematics

import scala.annotation.tailrec

/**
 * Monoid (take a deep breath, and relax about the weird name):
 *   This is a class that has an additive identify (called zero), and plus method that is
 *   associative: a+(b+c) = (a+b)+c and a+0=a, 0+a=a
 *
 * Group: this is a monoid that also has subtraction (and negation):
 *   So, you can do (a-b), or -a (which is equal to 0 - a).
 *
 * Ring: Group + multiplication (see: http://en.wikipedia.org/wiki/Ring_%28mathematics%29)
 *  and the three elements it defines:
 *  - additive identity aka zero
 *  - addition
 *  - multiplication
 *
 * The ring is to be passed as an argument to Matrix innerproduct and
 * provides the definitions for addition and multiplication
 *
 * Field: Ring + division. It is a generalization of Ring and adds support for inversion and
 *   multiplicative identity.
 */

trait Monoid[@specialized(Int,Long,Float,Double) T] extends java.io.Serializable {
  def zero : T //additive identity
  def assertNotZero(v : T) {
    if(zero == v) {
      throw new java.lang.IllegalArgumentException("argument should not be zero")
    }
  }

  def isNonZero(v : T) = (v != zero)

  def nonZeroOption(v : T): Option[T] = {
    if (isNonZero(v)) {
      Some(v)
    }
    else {
      None
    }
  }
  def plus(l : T, r : T) : T

  // Left sum: (((a + b) + c) + d)
  def sum(iter : Traversable[T]) : T = {
    iter.foldLeft(zero) { (old, current) => plus(old, current) }
  }
}

trait Group[@specialized(Int,Long,Float,Double) T] extends Monoid[T] {
  // must override negate or minus (or both)
  def negate(v : T) : T = minus(zero, v)
  def minus(l : T, r : T) : T = plus(l, negate(r))
}

trait Ring[@specialized(Int,Long,Float,Double) T] extends Group[T] {
  def one : T // Multiplicative identity
  def times(l : T, r : T) : T
  // Left product: (((a * b) * c) * d)
  def product(iter : Traversable[T]) : T = {
    iter.foldLeft(one) { (old, current) => times(old, current) }
  }
}

trait Field[@specialized(Int,Long,Float,Double) T] extends Ring[T] {
  // default implementation uses div YOU MUST OVERRIDE ONE OF THESE
  def inverse(v : T) : T = {
    assertNotZero(v)
    div(one, v)
  }
  // default implementation uses inverse:
  def div(l : T, r : T) : T = {
    assertNotZero(r)
    times(l, inverse(r))
  }
}

// TODO: this actually lifts a semigroup (no zero) into a Monoid.
// we should make a SemiGroup[T] class
class OptionMonoid[T](implicit mon : Monoid[T]) extends Monoid[Option[T]] {
  def zero = None
  def plus(left : Option[T], right : Option[T]) : Option[T] = {
    if(left.isEmpty) {
      right
    }
    else if(right.isEmpty) {
      left
    }
    else {
      Some(mon.plus(left.get, right.get))
    }
  }
}

/** Either monoid is useful for error handling.
 * if everything is correct, use Right (it's right, get it?), if something goes
 * wrong, use Left.  plus does the normal thing for plus(Right,Right), or plus(Left,Left),
 * but if exactly one is Left, we return that value (to keep the error condition).
 * Typically, the left value will be a string representing the errors.
 *
 * This actually works with a semigroup on L (we never explicitly touch zero in that group,
 * so, you can have a new Monoid[L] { def zero = sys.error("this is semigroup"); ...} and
 * everything would still work
 */
class EitherMonoid[L,R](implicit monoidl : Monoid[L], monoidr : Monoid[R])
  extends Monoid[Either[L,R]] {
  // TODO: remove this when we add a semi-group class
  override def zero = error("Either is a semi-group, there is no zero. Wrap with Option[Either[L,R]] to get a monoid.")
  override def plus(l : Either[L,R], r : Either[L,R]) = {
    if(l.isLeft) {
      // l is Left, r may or may not be:
      if(r.isRight) {
        //Avoid the allocation:
        l
      }
      else {
        //combine the lefts:
        Left(monoidl.plus(l.left.get, r.left.get))
      }
    }
    else if(r.isLeft) {
      //l is not a Left value, so just return right:
      r
    }
    else {
      //both l and r are Right values:
      Right(monoidr.plus(l.right.get, r.right.get))
    }
  }
}

object StringMonoid extends Monoid[String] {
  override val zero = ""
  override def plus(left : String, right : String) = left + right
}

/** List concatenation monoid.
 * plus means concatenation, zero is empty list
 */
class ListMonoid[T] extends Monoid[List[T]] {
  override def zero = List[T]()
  override def plus(left : List[T], right : List[T]) = left ++ right
}

/** A sorted-take List monoid (not the default, you can set:
 * implicit val sortmon = new SortedTakeListMonoid[T](10)
 * to use this instead of the standard list
 * This returns the k least values:
 * equivalent to: (left ++ right).sorted.take(k)
 * but doesn't do a total sort
 */
class SortedTakeListMonoid[T](k : Int)(implicit ord : Ordering[T]) extends Monoid[List[T]] {
  override def zero = List[T]()
  override def plus(left : List[T], right : List[T]) : List[T] = {
    //This is the internal loop that does one comparison:
    @tailrec
    def mergeSortR(acc : List[T], list1 : List[T], list2 : List[T], k : Int) : List[T] = {
      (list1, list2, k) match {
        case (_,_,0) => acc
        case (x1 :: t1, x2 :: t2, _) => {
          if( ord.lt(x1,x2) ) {
            mergeSortR(x1 :: acc, t1, list2, k-1)
          }
          else {
            mergeSortR(x2 :: acc, list1, t2, k-1)
          }
        }
        case (x1 :: t1, Nil, _) => mergeSortR(x1 :: acc, t1, Nil, k-1)
        case (Nil, x2 :: t2, _) => mergeSortR(x2 :: acc, Nil, t2, k-1)
        case (Nil, Nil, _) => acc
      }
    }
    mergeSortR(Nil, left, right, k).reverse
  }
}

/** Set union monoid.
 * plus means union, zero is empty set
 */
class SetMonoid[T] extends Monoid[Set[T]] {
  override def zero = Set[T]()
  override def plus(left : Set[T], right : Set[T]) = left ++ right
}

/** You can think of this as a Sparse vector monoid
 */
class MapMonoid[K,V](implicit monoid : Monoid[V]) extends Monoid[Map[K,V]] {
  override def zero = Map[K,V]()
  override def plus(left : Map[K,V], right : Map[K,V]) = {
    (left.keys.toSet ++ right.keys.toSet).foldLeft(Map[K,V]()) { (oldMap, newK) =>
      val leftV = left.getOrElse(newK, monoid.zero)
      val rightV = right.getOrElse(newK, monoid.zero)
      val newValue = monoid.plus(leftV, rightV)
      if (monoid.isNonZero(newValue)) {
        oldMap + (newK -> newValue)
      }
      else {
        //this key is already absent from oldMap
        oldMap
      }
    }
  }
  // This is not part of the typeclass, but it is useful:
  def sumValues(elem : Map[K,V]) : V = elem.values.reduceLeft { monoid.plus(_,_) }
}

class OptionMonoid[T](implicit monoid : Monoid[T]) extends Monoid[Option[T]] {
  override def zero : Option[T] = None
  override def plus(left : Option[T], right : Option[T]) = {
      if (left.isEmpty && right.isEmpty) {
        None
      } else {
        Some(monoid.plus(left.getOrElse(monoid.zero), right.getOrElse(monoid.zero)))
      }
  }
}

/** You can think of this as a Sparse vector group
 */
class MapGroup[K,V](implicit vgrp : Group[V]) extends MapMonoid[K,V]()(vgrp) with Group[Map[K,V]] {
  override def negate(kv : Map[K,V]) = kv.mapValues { v => vgrp.negate(v) }
}

/** You can think of this as a Sparse vector ring
 */
class MapRing[K,V](implicit ring : Ring[V]) extends MapGroup[K,V]()(ring) with Ring[Map[K,V]] {
  // It is possible to implement this, but we need a special "identity map" which we
  // deal with as if it were map with all possible keys (.get(x) == ring.one for all x).
  // Then we have to manage the delta from this map as we add elements.  That said, it
  // is not actually needed in matrix multiplication, so we are punting on it for now.
  override def one = error("multiplicative identity for Map unimplemented")
  override def times(left : Map[K,V], right : Map[K,V]) : Map[K,V] = {
    (left.keys.toSet & right.keys.toSet).foldLeft(Map[K,V]()) { (oldMap, newK) =>
      // The key is on both sides, so it is safe to get it out:
      val leftV = left(newK)
      val rightV = right(newK)
      val newValue = ring.times(leftV, rightV)
      if (ring.isNonZero(newValue)) {
        oldMap + (newK -> newValue)
      }
      else {
        // Keep it sparse
        oldMap - newK
      }
    }
  }
  // This is not part of the typeclass, but it is useful:
  def productValues(elem : Map[K,V]) : V = elem.values.reduceLeft { ring.times(_,_) }
  def dot(left : Map[K,V], right : Map[K,V]) : V = sumValues(times(left, right))
}

object IntRing extends Ring[Int] {
  override def zero = 0
  override def one = 1
  override def negate(v : Int) = -v
  override def plus(l : Int, r : Int) = l + r
  override def minus(l : Int, r : Int) = l - r
  override def times(l : Int, r : Int) = l * r
}

object LongRing extends Ring[Long] {
  override def zero = 0L
  override def one = 1L
  override def negate(v : Long) = -v
  override def plus(l : Long, r : Long) = l + r
  override def minus(l : Long, r : Long) = l - r
  override def times(l : Long, r : Long) = l * r
}

object FloatField extends Field[Float] {
  override def one = 1.0f
  override def zero = 0.0f
  override def negate(v : Float) = -v
  override def plus(l : Float, r : Float) = l + r
  override def minus(l : Float, r : Float) = l - r
  override def times(l : Float, r : Float) = l * r
  override def div(l : Float, r : Float) = {
    assertNotZero(r)
    l / r
  }
}

object DoubleField extends Field[Double] {
  override def one = 1.0
  override def zero = 0.0
  override def negate(v : Double) = -v
  override def plus(l : Double, r : Double) = l + r
  override def minus(l : Double, r : Double) = l - r
  override def times(l : Double, r : Double) = l * r
  override def div(l : Double, r : Double) = {
    assertNotZero(r)
    l / r
  }
}

object BooleanField extends Field[Boolean] {
  override def one = true
  override def zero = false
  override def negate(v : Boolean) = v
  override def plus(l : Boolean, r : Boolean) = l ^ r
  override def minus(l : Boolean, r : Boolean) = l ^ r
  override def times(l : Boolean, r : Boolean) = l && r
  override def inverse(l : Boolean) = {
    assertNotZero(l)
    true
  }
  override def div(l : Boolean, r : Boolean) = {
    assertNotZero(r)
    l
  }
}

// Trivial group, but possibly useful to make a group of (Unit, T) for some T.
object UnitGroup extends Group[Unit] {
  override def zero = ()
  override def negate(u : Unit) = ()
  override def plus(l : Unit, r : Unit) = ()
}

// similar to the above:
object NullGroup extends Group[Null] {
  override def zero = null
  override def negate(u : Null) = null
  override def plus(l : Null, r : Null) = null
}

/**
* Combine two monoids into a product monoid
*/
class Tuple2Monoid[T,U](implicit tmonoid : Monoid[T], umonoid : Monoid[U]) extends Monoid[(T,U)] {
  override def zero = (tmonoid.zero, umonoid.zero)
  override def plus(l : (T,U), r : (T,U)) = (tmonoid.plus(l._1,r._1), umonoid.plus(l._2, r._2))
}

/**
* Combine two groups into a product group
*/
class Tuple2Group[T,U](implicit tgroup : Group[T], ugroup : Group[U]) extends Group[(T,U)] {
  override def zero = (tgroup.zero, ugroup.zero)
  override def negate(v : (T,U)) = (tgroup.negate(v._1), ugroup.negate(v._2))
  override def plus(l : (T,U), r : (T,U)) = (tgroup.plus(l._1,r._1), ugroup.plus(l._2, r._2))
  override def minus(l : (T,U), r : (T,U)) = (tgroup.minus(l._1,r._1), ugroup.minus(l._2, r._2))
}

/**
* Combine two rings into a product ring
*/
class Tuple2Ring[T,U](implicit tring : Ring[T], uring : Ring[U]) extends Ring[(T,U)] {
  override def zero = (tring.zero, uring.zero)
  override def one = (tring.one, uring.one)
  override def negate(v : (T,U)) = (tring.negate(v._1), uring.negate(v._2))
  override def plus(l : (T,U), r : (T,U)) = (tring.plus(l._1,r._1), uring.plus(l._2, r._2))
  override def minus(l : (T,U), r : (T,U)) = (tring.minus(l._1,r._1), uring.minus(l._2, r._2))
  override def times(l : (T,U), r : (T,U)) = (tring.times(l._1,r._1), uring.times(l._2, r._2))
}

object Monoid extends GeneratedMonoidImplicits {
  implicit val nullMonoid : Monoid[Null] = NullGroup
  implicit val unitMonoid : Monoid[Unit] = UnitGroup
  implicit val boolMonoid : Monoid[Boolean] = BooleanField
  implicit val intMonoid : Monoid[Int] = IntRing
  implicit val longMonoid : Monoid[Long] = LongRing
  implicit val floatMonoid : Monoid[Float] = FloatField
  implicit val doubleMonoid : Monoid[Double] = DoubleField
  implicit val stringMonoid : Monoid[String] = StringMonoid
  implicit def optionMonoid[T : Monoid] = new OptionMonoid[T]
  implicit def listMonoid[T] : Monoid[List[T]] = new ListMonoid[T]
  implicit def setMonoid[T] : Monoid[Set[T]] = new SetMonoid[T]
  implicit def mapMonoid[K,V](implicit monoid : Monoid[V]) = new MapMonoid[K,V]()(monoid)
  implicit def pairMonoid[T,U](implicit tg : Monoid[T], ug : Monoid[U]) : Monoid[(T,U)] = {
    new Tuple2Monoid[T,U]()(tg,ug)
  }
  implicit def eitherMonoid[L : Monoid, R : Monoid] = new EitherMonoid[L,R]
}

object Group extends GeneratedGroupImplicits {
  implicit val nullGroup : Group[Null] = NullGroup
  implicit val unitGroup : Group[Unit] = UnitGroup
  implicit val boolGroup : Group[Boolean] = BooleanField
  implicit val intGroup : Group[Int] = IntRing
  implicit val longGroup : Group[Long] = LongRing
  implicit val floatGroup : Group[Float] = FloatField
  implicit val doubleGroup : Group[Double] = DoubleField
  implicit def mapGroup[K,V](implicit group : Group[V]) = new MapGroup[K,V]()(group)
  implicit def pairGroup[T,U](implicit tg : Group[T], ug : Group[U]) : Group[(T,U)] = {
    new Tuple2Group[T,U]()(tg,ug)
  }
}

object Ring extends GeneratedRingImplicits {
  implicit val boolRing : Ring[Boolean] = BooleanField
  implicit val intRing : Ring[Int] = IntRing
  implicit val longRing : Ring[Long] = LongRing
  implicit val floatRing : Ring[Float] = FloatField
  implicit val doubleRing : Ring[Double] = DoubleField
  implicit def mapRing[K,V](implicit ring : Ring[V]) = new MapRing[K,V]()(ring)
  implicit def pairRing[T,U](implicit tr : Ring[T], ur : Ring[U]) : Ring[(T,U)] = {
    new Tuple2Ring[T,U]()(tr,ur)
  }
}

object Field {
  implicit val boolField : Field[Boolean] = BooleanField
  implicit val floatField : Field[Float] = FloatField
  implicit val doubleField : Field[Double] = DoubleField
}
