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
}

trait Group[@specialized(Int,Long,Float,Double) T] extends Monoid[T] {
  // must override negate or minus (or both)
  def negate(v : T) : T = minus(zero, v)
  def minus(l : T, r : T) : T = plus(l, negate(r))
}

trait Ring[@specialized(Int,Long,Float,Double) T] extends Group[T] {
  def one : T // Multiplicative identity
  def times(l : T, r : T) : T
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

/** List concatenation monoid.
 * plus means concatenation, zero is empty list
 */
class ListMonoid[T] extends Monoid[List[T]] {
  override def zero = List[T]()
  override def plus(left : List[T], right : List[T]) = left ++ right
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
        // Keep it sparse
        oldMap - newK
      }
    }
  }
  // This is not part of the typeclass, but it is useful:
  def sumValues(elem : Map[K,V]) : V = elem.values.reduceLeft { monoid.plus(_,_) }
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

/**
* Combine 3 monoids into a product monoid
*/
class Tuple3Monoid[A, B, C](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C]) extends Monoid[(A, B, C)] {
  override def zero = (amonoid.zero, bmonoid.zero, cmonoid.zero)
  override def plus(l : (A, B, C), r : (A, B, C)) = (amonoid.plus(l._1, r._1), bmonoid.plus(l._2, r._2), cmonoid.plus(l._3, r._3))
}

/**
* Combine 3 groups into a product group
*/
class Tuple3Group[A, B, C](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C]) extends Group[(A, B, C)] {
  override def zero = (agroup.zero, bgroup.zero, cgroup.zero)
  override def negate(v : (A, B, C)) = (agroup.negate(v._1), bgroup.negate(v._2), cgroup.negate(v._3))
  override def plus(l : (A, B, C), r : (A, B, C)) = (agroup.plus(l._1, r._1), bgroup.plus(l._2, r._2), cgroup.plus(l._3, r._3))
  override def minus(l : (A, B, C), r : (A, B, C)) = (agroup.minus(l._1, r._1), bgroup.minus(l._2, r._2), cgroup.minus(l._3, r._3))
}

/**
* Combine 3 rings into a product ring
*/
class Tuple3Ring[A, B, C](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C]) extends Ring[(A, B, C)] {
  override def zero = (aring.zero, bring.zero, cring.zero)
  override def one = (aring.one, bring.one, cring.one)
  override def negate(v : (A, B, C)) = (aring.negate(v._1), bring.negate(v._2), cring.negate(v._3))
  override def plus(l : (A, B, C), r : (A, B, C)) = (aring.plus(l._1, r._1), bring.plus(l._2, r._2), cring.plus(l._3, r._3))
  override def minus(l : (A, B, C), r : (A, B, C)) = (aring.minus(l._1, r._1), bring.minus(l._2, r._2), cring.minus(l._3, r._3))
  override def times(l : (A, B, C), r : (A, B, C)) = (aring.times(l._1, r._1), bring.times(l._2, r._2), cring.times(l._3, r._3))
}
/**
* Combine 4 monoids into a product monoid
*/
class Tuple4Monoid[A, B, C, D](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D]) extends Monoid[(A, B, C, D)] {
  override def zero = (amonoid.zero, bmonoid.zero, cmonoid.zero, dmonoid.zero)
  override def plus(l : (A, B, C, D), r : (A, B, C, D)) = (amonoid.plus(l._1, r._1), bmonoid.plus(l._2, r._2), cmonoid.plus(l._3, r._3), dmonoid.plus(l._4, r._4))
}

/**
* Combine 4 groups into a product group
*/
class Tuple4Group[A, B, C, D](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D]) extends Group[(A, B, C, D)] {
  override def zero = (agroup.zero, bgroup.zero, cgroup.zero, dgroup.zero)
  override def negate(v : (A, B, C, D)) = (agroup.negate(v._1), bgroup.negate(v._2), cgroup.negate(v._3), dgroup.negate(v._4))
  override def plus(l : (A, B, C, D), r : (A, B, C, D)) = (agroup.plus(l._1, r._1), bgroup.plus(l._2, r._2), cgroup.plus(l._3, r._3), dgroup.plus(l._4, r._4))
  override def minus(l : (A, B, C, D), r : (A, B, C, D)) = (agroup.minus(l._1, r._1), bgroup.minus(l._2, r._2), cgroup.minus(l._3, r._3), dgroup.minus(l._4, r._4))
}

/**
* Combine 4 rings into a product ring
*/
class Tuple4Ring[A, B, C, D](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D]) extends Ring[(A, B, C, D)] {
  override def zero = (aring.zero, bring.zero, cring.zero, dring.zero)
  override def one = (aring.one, bring.one, cring.one, dring.one)
  override def negate(v : (A, B, C, D)) = (aring.negate(v._1), bring.negate(v._2), cring.negate(v._3), dring.negate(v._4))
  override def plus(l : (A, B, C, D), r : (A, B, C, D)) = (aring.plus(l._1, r._1), bring.plus(l._2, r._2), cring.plus(l._3, r._3), dring.plus(l._4, r._4))
  override def minus(l : (A, B, C, D), r : (A, B, C, D)) = (aring.minus(l._1, r._1), bring.minus(l._2, r._2), cring.minus(l._3, r._3), dring.minus(l._4, r._4))
  override def times(l : (A, B, C, D), r : (A, B, C, D)) = (aring.times(l._1, r._1), bring.times(l._2, r._2), cring.times(l._3, r._3), dring.times(l._4, r._4))
}
/**
* Combine 5 monoids into a product monoid
*/
class Tuple5Monoid[A, B, C, D, E](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E]) extends Monoid[(A, B, C, D, E)] {
  override def zero = (amonoid.zero, bmonoid.zero, cmonoid.zero, dmonoid.zero, emonoid.zero)
  override def plus(l : (A, B, C, D, E), r : (A, B, C, D, E)) = (amonoid.plus(l._1, r._1), bmonoid.plus(l._2, r._2), cmonoid.plus(l._3, r._3), dmonoid.plus(l._4, r._4), emonoid.plus(l._5, r._5))
}

/**
* Combine 5 groups into a product group
*/
class Tuple5Group[A, B, C, D, E](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E]) extends Group[(A, B, C, D, E)] {
  override def zero = (agroup.zero, bgroup.zero, cgroup.zero, dgroup.zero, egroup.zero)
  override def negate(v : (A, B, C, D, E)) = (agroup.negate(v._1), bgroup.negate(v._2), cgroup.negate(v._3), dgroup.negate(v._4), egroup.negate(v._5))
  override def plus(l : (A, B, C, D, E), r : (A, B, C, D, E)) = (agroup.plus(l._1, r._1), bgroup.plus(l._2, r._2), cgroup.plus(l._3, r._3), dgroup.plus(l._4, r._4), egroup.plus(l._5, r._5))
  override def minus(l : (A, B, C, D, E), r : (A, B, C, D, E)) = (agroup.minus(l._1, r._1), bgroup.minus(l._2, r._2), cgroup.minus(l._3, r._3), dgroup.minus(l._4, r._4), egroup.minus(l._5, r._5))
}

/**
* Combine 5 rings into a product ring
*/
class Tuple5Ring[A, B, C, D, E](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E]) extends Ring[(A, B, C, D, E)] {
  override def zero = (aring.zero, bring.zero, cring.zero, dring.zero, ering.zero)
  override def one = (aring.one, bring.one, cring.one, dring.one, ering.one)
  override def negate(v : (A, B, C, D, E)) = (aring.negate(v._1), bring.negate(v._2), cring.negate(v._3), dring.negate(v._4), ering.negate(v._5))
  override def plus(l : (A, B, C, D, E), r : (A, B, C, D, E)) = (aring.plus(l._1, r._1), bring.plus(l._2, r._2), cring.plus(l._3, r._3), dring.plus(l._4, r._4), ering.plus(l._5, r._5))
  override def minus(l : (A, B, C, D, E), r : (A, B, C, D, E)) = (aring.minus(l._1, r._1), bring.minus(l._2, r._2), cring.minus(l._3, r._3), dring.minus(l._4, r._4), ering.minus(l._5, r._5))
  override def times(l : (A, B, C, D, E), r : (A, B, C, D, E)) = (aring.times(l._1, r._1), bring.times(l._2, r._2), cring.times(l._3, r._3), dring.times(l._4, r._4), ering.times(l._5, r._5))
}
/**
* Combine 6 monoids into a product monoid
*/
class Tuple6Monoid[A, B, C, D, E, F](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F]) extends Monoid[(A, B, C, D, E, F)] {
  override def zero = (amonoid.zero, bmonoid.zero, cmonoid.zero, dmonoid.zero, emonoid.zero, fmonoid.zero)
  override def plus(l : (A, B, C, D, E, F), r : (A, B, C, D, E, F)) = (amonoid.plus(l._1, r._1), bmonoid.plus(l._2, r._2), cmonoid.plus(l._3, r._3), dmonoid.plus(l._4, r._4), emonoid.plus(l._5, r._5), fmonoid.plus(l._6, r._6))
}

/**
* Combine 6 groups into a product group
*/
class Tuple6Group[A, B, C, D, E, F](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F]) extends Group[(A, B, C, D, E, F)] {
  override def zero = (agroup.zero, bgroup.zero, cgroup.zero, dgroup.zero, egroup.zero, fgroup.zero)
  override def negate(v : (A, B, C, D, E, F)) = (agroup.negate(v._1), bgroup.negate(v._2), cgroup.negate(v._3), dgroup.negate(v._4), egroup.negate(v._5), fgroup.negate(v._6))
  override def plus(l : (A, B, C, D, E, F), r : (A, B, C, D, E, F)) = (agroup.plus(l._1, r._1), bgroup.plus(l._2, r._2), cgroup.plus(l._3, r._3), dgroup.plus(l._4, r._4), egroup.plus(l._5, r._5), fgroup.plus(l._6, r._6))
  override def minus(l : (A, B, C, D, E, F), r : (A, B, C, D, E, F)) = (agroup.minus(l._1, r._1), bgroup.minus(l._2, r._2), cgroup.minus(l._3, r._3), dgroup.minus(l._4, r._4), egroup.minus(l._5, r._5), fgroup.minus(l._6, r._6))
}

/**
* Combine 6 rings into a product ring
*/
class Tuple6Ring[A, B, C, D, E, F](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F]) extends Ring[(A, B, C, D, E, F)] {
  override def zero = (aring.zero, bring.zero, cring.zero, dring.zero, ering.zero, fring.zero)
  override def one = (aring.one, bring.one, cring.one, dring.one, ering.one, fring.one)
  override def negate(v : (A, B, C, D, E, F)) = (aring.negate(v._1), bring.negate(v._2), cring.negate(v._3), dring.negate(v._4), ering.negate(v._5), fring.negate(v._6))
  override def plus(l : (A, B, C, D, E, F), r : (A, B, C, D, E, F)) = (aring.plus(l._1, r._1), bring.plus(l._2, r._2), cring.plus(l._3, r._3), dring.plus(l._4, r._4), ering.plus(l._5, r._5), fring.plus(l._6, r._6))
  override def minus(l : (A, B, C, D, E, F), r : (A, B, C, D, E, F)) = (aring.minus(l._1, r._1), bring.minus(l._2, r._2), cring.minus(l._3, r._3), dring.minus(l._4, r._4), ering.minus(l._5, r._5), fring.minus(l._6, r._6))
  override def times(l : (A, B, C, D, E, F), r : (A, B, C, D, E, F)) = (aring.times(l._1, r._1), bring.times(l._2, r._2), cring.times(l._3, r._3), dring.times(l._4, r._4), ering.times(l._5, r._5), fring.times(l._6, r._6))
}
/**
* Combine 7 monoids into a product monoid
*/
class Tuple7Monoid[A, B, C, D, E, F, G](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G]) extends Monoid[(A, B, C, D, E, F, G)] {
  override def zero = (amonoid.zero, bmonoid.zero, cmonoid.zero, dmonoid.zero, emonoid.zero, fmonoid.zero, gmonoid.zero)
  override def plus(l : (A, B, C, D, E, F, G), r : (A, B, C, D, E, F, G)) = (amonoid.plus(l._1, r._1), bmonoid.plus(l._2, r._2), cmonoid.plus(l._3, r._3), dmonoid.plus(l._4, r._4), emonoid.plus(l._5, r._5), fmonoid.plus(l._6, r._6), gmonoid.plus(l._7, r._7))
}

/**
* Combine 7 groups into a product group
*/
class Tuple7Group[A, B, C, D, E, F, G](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G]) extends Group[(A, B, C, D, E, F, G)] {
  override def zero = (agroup.zero, bgroup.zero, cgroup.zero, dgroup.zero, egroup.zero, fgroup.zero, ggroup.zero)
  override def negate(v : (A, B, C, D, E, F, G)) = (agroup.negate(v._1), bgroup.negate(v._2), cgroup.negate(v._3), dgroup.negate(v._4), egroup.negate(v._5), fgroup.negate(v._6), ggroup.negate(v._7))
  override def plus(l : (A, B, C, D, E, F, G), r : (A, B, C, D, E, F, G)) = (agroup.plus(l._1, r._1), bgroup.plus(l._2, r._2), cgroup.plus(l._3, r._3), dgroup.plus(l._4, r._4), egroup.plus(l._5, r._5), fgroup.plus(l._6, r._6), ggroup.plus(l._7, r._7))
  override def minus(l : (A, B, C, D, E, F, G), r : (A, B, C, D, E, F, G)) = (agroup.minus(l._1, r._1), bgroup.minus(l._2, r._2), cgroup.minus(l._3, r._3), dgroup.minus(l._4, r._4), egroup.minus(l._5, r._5), fgroup.minus(l._6, r._6), ggroup.minus(l._7, r._7))
}

/**
* Combine 7 rings into a product ring
*/
class Tuple7Ring[A, B, C, D, E, F, G](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G]) extends Ring[(A, B, C, D, E, F, G)] {
  override def zero = (aring.zero, bring.zero, cring.zero, dring.zero, ering.zero, fring.zero, gring.zero)
  override def one = (aring.one, bring.one, cring.one, dring.one, ering.one, fring.one, gring.one)
  override def negate(v : (A, B, C, D, E, F, G)) = (aring.negate(v._1), bring.negate(v._2), cring.negate(v._3), dring.negate(v._4), ering.negate(v._5), fring.negate(v._6), gring.negate(v._7))
  override def plus(l : (A, B, C, D, E, F, G), r : (A, B, C, D, E, F, G)) = (aring.plus(l._1, r._1), bring.plus(l._2, r._2), cring.plus(l._3, r._3), dring.plus(l._4, r._4), ering.plus(l._5, r._5), fring.plus(l._6, r._6), gring.plus(l._7, r._7))
  override def minus(l : (A, B, C, D, E, F, G), r : (A, B, C, D, E, F, G)) = (aring.minus(l._1, r._1), bring.minus(l._2, r._2), cring.minus(l._3, r._3), dring.minus(l._4, r._4), ering.minus(l._5, r._5), fring.minus(l._6, r._6), gring.minus(l._7, r._7))
  override def times(l : (A, B, C, D, E, F, G), r : (A, B, C, D, E, F, G)) = (aring.times(l._1, r._1), bring.times(l._2, r._2), cring.times(l._3, r._3), dring.times(l._4, r._4), ering.times(l._5, r._5), fring.times(l._6, r._6), gring.times(l._7, r._7))
}
/**
* Combine 8 monoids into a product monoid
*/
class Tuple8Monoid[A, B, C, D, E, F, G, H](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H]) extends Monoid[(A, B, C, D, E, F, G, H)] {
  override def zero = (amonoid.zero, bmonoid.zero, cmonoid.zero, dmonoid.zero, emonoid.zero, fmonoid.zero, gmonoid.zero, hmonoid.zero)
  override def plus(l : (A, B, C, D, E, F, G, H), r : (A, B, C, D, E, F, G, H)) = (amonoid.plus(l._1, r._1), bmonoid.plus(l._2, r._2), cmonoid.plus(l._3, r._3), dmonoid.plus(l._4, r._4), emonoid.plus(l._5, r._5), fmonoid.plus(l._6, r._6), gmonoid.plus(l._7, r._7), hmonoid.plus(l._8, r._8))
}

/**
* Combine 8 groups into a product group
*/
class Tuple8Group[A, B, C, D, E, F, G, H](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H]) extends Group[(A, B, C, D, E, F, G, H)] {
  override def zero = (agroup.zero, bgroup.zero, cgroup.zero, dgroup.zero, egroup.zero, fgroup.zero, ggroup.zero, hgroup.zero)
  override def negate(v : (A, B, C, D, E, F, G, H)) = (agroup.negate(v._1), bgroup.negate(v._2), cgroup.negate(v._3), dgroup.negate(v._4), egroup.negate(v._5), fgroup.negate(v._6), ggroup.negate(v._7), hgroup.negate(v._8))
  override def plus(l : (A, B, C, D, E, F, G, H), r : (A, B, C, D, E, F, G, H)) = (agroup.plus(l._1, r._1), bgroup.plus(l._2, r._2), cgroup.plus(l._3, r._3), dgroup.plus(l._4, r._4), egroup.plus(l._5, r._5), fgroup.plus(l._6, r._6), ggroup.plus(l._7, r._7), hgroup.plus(l._8, r._8))
  override def minus(l : (A, B, C, D, E, F, G, H), r : (A, B, C, D, E, F, G, H)) = (agroup.minus(l._1, r._1), bgroup.minus(l._2, r._2), cgroup.minus(l._3, r._3), dgroup.minus(l._4, r._4), egroup.minus(l._5, r._5), fgroup.minus(l._6, r._6), ggroup.minus(l._7, r._7), hgroup.minus(l._8, r._8))
}

/**
* Combine 8 rings into a product ring
*/
class Tuple8Ring[A, B, C, D, E, F, G, H](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H]) extends Ring[(A, B, C, D, E, F, G, H)] {
  override def zero = (aring.zero, bring.zero, cring.zero, dring.zero, ering.zero, fring.zero, gring.zero, hring.zero)
  override def one = (aring.one, bring.one, cring.one, dring.one, ering.one, fring.one, gring.one, hring.one)
  override def negate(v : (A, B, C, D, E, F, G, H)) = (aring.negate(v._1), bring.negate(v._2), cring.negate(v._3), dring.negate(v._4), ering.negate(v._5), fring.negate(v._6), gring.negate(v._7), hring.negate(v._8))
  override def plus(l : (A, B, C, D, E, F, G, H), r : (A, B, C, D, E, F, G, H)) = (aring.plus(l._1, r._1), bring.plus(l._2, r._2), cring.plus(l._3, r._3), dring.plus(l._4, r._4), ering.plus(l._5, r._5), fring.plus(l._6, r._6), gring.plus(l._7, r._7), hring.plus(l._8, r._8))
  override def minus(l : (A, B, C, D, E, F, G, H), r : (A, B, C, D, E, F, G, H)) = (aring.minus(l._1, r._1), bring.minus(l._2, r._2), cring.minus(l._3, r._3), dring.minus(l._4, r._4), ering.minus(l._5, r._5), fring.minus(l._6, r._6), gring.minus(l._7, r._7), hring.minus(l._8, r._8))
  override def times(l : (A, B, C, D, E, F, G, H), r : (A, B, C, D, E, F, G, H)) = (aring.times(l._1, r._1), bring.times(l._2, r._2), cring.times(l._3, r._3), dring.times(l._4, r._4), ering.times(l._5, r._5), fring.times(l._6, r._6), gring.times(l._7, r._7), hring.times(l._8, r._8))
}
/**
* Combine 9 monoids into a product monoid
*/
class Tuple9Monoid[A, B, C, D, E, F, G, H, I](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I]) extends Monoid[(A, B, C, D, E, F, G, H, I)] {
  override def zero = (amonoid.zero, bmonoid.zero, cmonoid.zero, dmonoid.zero, emonoid.zero, fmonoid.zero, gmonoid.zero, hmonoid.zero, imonoid.zero)
  override def plus(l : (A, B, C, D, E, F, G, H, I), r : (A, B, C, D, E, F, G, H, I)) = (amonoid.plus(l._1, r._1), bmonoid.plus(l._2, r._2), cmonoid.plus(l._3, r._3), dmonoid.plus(l._4, r._4), emonoid.plus(l._5, r._5), fmonoid.plus(l._6, r._6), gmonoid.plus(l._7, r._7), hmonoid.plus(l._8, r._8), imonoid.plus(l._9, r._9))
}

/**
* Combine 9 groups into a product group
*/
class Tuple9Group[A, B, C, D, E, F, G, H, I](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I]) extends Group[(A, B, C, D, E, F, G, H, I)] {
  override def zero = (agroup.zero, bgroup.zero, cgroup.zero, dgroup.zero, egroup.zero, fgroup.zero, ggroup.zero, hgroup.zero, igroup.zero)
  override def negate(v : (A, B, C, D, E, F, G, H, I)) = (agroup.negate(v._1), bgroup.negate(v._2), cgroup.negate(v._3), dgroup.negate(v._4), egroup.negate(v._5), fgroup.negate(v._6), ggroup.negate(v._7), hgroup.negate(v._8), igroup.negate(v._9))
  override def plus(l : (A, B, C, D, E, F, G, H, I), r : (A, B, C, D, E, F, G, H, I)) = (agroup.plus(l._1, r._1), bgroup.plus(l._2, r._2), cgroup.plus(l._3, r._3), dgroup.plus(l._4, r._4), egroup.plus(l._5, r._5), fgroup.plus(l._6, r._6), ggroup.plus(l._7, r._7), hgroup.plus(l._8, r._8), igroup.plus(l._9, r._9))
  override def minus(l : (A, B, C, D, E, F, G, H, I), r : (A, B, C, D, E, F, G, H, I)) = (agroup.minus(l._1, r._1), bgroup.minus(l._2, r._2), cgroup.minus(l._3, r._3), dgroup.minus(l._4, r._4), egroup.minus(l._5, r._5), fgroup.minus(l._6, r._6), ggroup.minus(l._7, r._7), hgroup.minus(l._8, r._8), igroup.minus(l._9, r._9))
}

/**
* Combine 9 rings into a product ring
*/
class Tuple9Ring[A, B, C, D, E, F, G, H, I](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I]) extends Ring[(A, B, C, D, E, F, G, H, I)] {
  override def zero = (aring.zero, bring.zero, cring.zero, dring.zero, ering.zero, fring.zero, gring.zero, hring.zero, iring.zero)
  override def one = (aring.one, bring.one, cring.one, dring.one, ering.one, fring.one, gring.one, hring.one, iring.one)
  override def negate(v : (A, B, C, D, E, F, G, H, I)) = (aring.negate(v._1), bring.negate(v._2), cring.negate(v._3), dring.negate(v._4), ering.negate(v._5), fring.negate(v._6), gring.negate(v._7), hring.negate(v._8), iring.negate(v._9))
  override def plus(l : (A, B, C, D, E, F, G, H, I), r : (A, B, C, D, E, F, G, H, I)) = (aring.plus(l._1, r._1), bring.plus(l._2, r._2), cring.plus(l._3, r._3), dring.plus(l._4, r._4), ering.plus(l._5, r._5), fring.plus(l._6, r._6), gring.plus(l._7, r._7), hring.plus(l._8, r._8), iring.plus(l._9, r._9))
  override def minus(l : (A, B, C, D, E, F, G, H, I), r : (A, B, C, D, E, F, G, H, I)) = (aring.minus(l._1, r._1), bring.minus(l._2, r._2), cring.minus(l._3, r._3), dring.minus(l._4, r._4), ering.minus(l._5, r._5), fring.minus(l._6, r._6), gring.minus(l._7, r._7), hring.minus(l._8, r._8), iring.minus(l._9, r._9))
  override def times(l : (A, B, C, D, E, F, G, H, I), r : (A, B, C, D, E, F, G, H, I)) = (aring.times(l._1, r._1), bring.times(l._2, r._2), cring.times(l._3, r._3), dring.times(l._4, r._4), ering.times(l._5, r._5), fring.times(l._6, r._6), gring.times(l._7, r._7), hring.times(l._8, r._8), iring.times(l._9, r._9))
}
/**
* Combine 10 monoids into a product monoid
*/
class Tuple10Monoid[A, B, C, D, E, F, G, H, I, J](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I], jmonoid : Monoid[J]) extends Monoid[(A, B, C, D, E, F, G, H, I, J)] {
  override def zero = (amonoid.zero, bmonoid.zero, cmonoid.zero, dmonoid.zero, emonoid.zero, fmonoid.zero, gmonoid.zero, hmonoid.zero, imonoid.zero, jmonoid.zero)
  override def plus(l : (A, B, C, D, E, F, G, H, I, J), r : (A, B, C, D, E, F, G, H, I, J)) = (amonoid.plus(l._1, r._1), bmonoid.plus(l._2, r._2), cmonoid.plus(l._3, r._3), dmonoid.plus(l._4, r._4), emonoid.plus(l._5, r._5), fmonoid.plus(l._6, r._6), gmonoid.plus(l._7, r._7), hmonoid.plus(l._8, r._8), imonoid.plus(l._9, r._9), jmonoid.plus(l._10, r._10))
}

/**
* Combine 10 groups into a product group
*/
class Tuple10Group[A, B, C, D, E, F, G, H, I, J](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I], jgroup : Group[J]) extends Group[(A, B, C, D, E, F, G, H, I, J)] {
  override def zero = (agroup.zero, bgroup.zero, cgroup.zero, dgroup.zero, egroup.zero, fgroup.zero, ggroup.zero, hgroup.zero, igroup.zero, jgroup.zero)
  override def negate(v : (A, B, C, D, E, F, G, H, I, J)) = (agroup.negate(v._1), bgroup.negate(v._2), cgroup.negate(v._3), dgroup.negate(v._4), egroup.negate(v._5), fgroup.negate(v._6), ggroup.negate(v._7), hgroup.negate(v._8), igroup.negate(v._9), jgroup.negate(v._10))
  override def plus(l : (A, B, C, D, E, F, G, H, I, J), r : (A, B, C, D, E, F, G, H, I, J)) = (agroup.plus(l._1, r._1), bgroup.plus(l._2, r._2), cgroup.plus(l._3, r._3), dgroup.plus(l._4, r._4), egroup.plus(l._5, r._5), fgroup.plus(l._6, r._6), ggroup.plus(l._7, r._7), hgroup.plus(l._8, r._8), igroup.plus(l._9, r._9), jgroup.plus(l._10, r._10))
  override def minus(l : (A, B, C, D, E, F, G, H, I, J), r : (A, B, C, D, E, F, G, H, I, J)) = (agroup.minus(l._1, r._1), bgroup.minus(l._2, r._2), cgroup.minus(l._3, r._3), dgroup.minus(l._4, r._4), egroup.minus(l._5, r._5), fgroup.minus(l._6, r._6), ggroup.minus(l._7, r._7), hgroup.minus(l._8, r._8), igroup.minus(l._9, r._9), jgroup.minus(l._10, r._10))
}

/**
* Combine 10 rings into a product ring
*/
class Tuple10Ring[A, B, C, D, E, F, G, H, I, J](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I], jring : Ring[J]) extends Ring[(A, B, C, D, E, F, G, H, I, J)] {
  override def zero = (aring.zero, bring.zero, cring.zero, dring.zero, ering.zero, fring.zero, gring.zero, hring.zero, iring.zero, jring.zero)
  override def one = (aring.one, bring.one, cring.one, dring.one, ering.one, fring.one, gring.one, hring.one, iring.one, jring.one)
  override def negate(v : (A, B, C, D, E, F, G, H, I, J)) = (aring.negate(v._1), bring.negate(v._2), cring.negate(v._3), dring.negate(v._4), ering.negate(v._5), fring.negate(v._6), gring.negate(v._7), hring.negate(v._8), iring.negate(v._9), jring.negate(v._10))
  override def plus(l : (A, B, C, D, E, F, G, H, I, J), r : (A, B, C, D, E, F, G, H, I, J)) = (aring.plus(l._1, r._1), bring.plus(l._2, r._2), cring.plus(l._3, r._3), dring.plus(l._4, r._4), ering.plus(l._5, r._5), fring.plus(l._6, r._6), gring.plus(l._7, r._7), hring.plus(l._8, r._8), iring.plus(l._9, r._9), jring.plus(l._10, r._10))
  override def minus(l : (A, B, C, D, E, F, G, H, I, J), r : (A, B, C, D, E, F, G, H, I, J)) = (aring.minus(l._1, r._1), bring.minus(l._2, r._2), cring.minus(l._3, r._3), dring.minus(l._4, r._4), ering.minus(l._5, r._5), fring.minus(l._6, r._6), gring.minus(l._7, r._7), hring.minus(l._8, r._8), iring.minus(l._9, r._9), jring.minus(l._10, r._10))
  override def times(l : (A, B, C, D, E, F, G, H, I, J), r : (A, B, C, D, E, F, G, H, I, J)) = (aring.times(l._1, r._1), bring.times(l._2, r._2), cring.times(l._3, r._3), dring.times(l._4, r._4), ering.times(l._5, r._5), fring.times(l._6, r._6), gring.times(l._7, r._7), hring.times(l._8, r._8), iring.times(l._9, r._9), jring.times(l._10, r._10))
}
/**
* Combine 11 monoids into a product monoid
*/
class Tuple11Monoid[A, B, C, D, E, F, G, H, I, J, K](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I], jmonoid : Monoid[J], kmonoid : Monoid[K]) extends Monoid[(A, B, C, D, E, F, G, H, I, J, K)] {
  override def zero = (amonoid.zero, bmonoid.zero, cmonoid.zero, dmonoid.zero, emonoid.zero, fmonoid.zero, gmonoid.zero, hmonoid.zero, imonoid.zero, jmonoid.zero, kmonoid.zero)
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K), r : (A, B, C, D, E, F, G, H, I, J, K)) = (amonoid.plus(l._1, r._1), bmonoid.plus(l._2, r._2), cmonoid.plus(l._3, r._3), dmonoid.plus(l._4, r._4), emonoid.plus(l._5, r._5), fmonoid.plus(l._6, r._6), gmonoid.plus(l._7, r._7), hmonoid.plus(l._8, r._8), imonoid.plus(l._9, r._9), jmonoid.plus(l._10, r._10), kmonoid.plus(l._11, r._11))
}

/**
* Combine 11 groups into a product group
*/
class Tuple11Group[A, B, C, D, E, F, G, H, I, J, K](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I], jgroup : Group[J], kgroup : Group[K]) extends Group[(A, B, C, D, E, F, G, H, I, J, K)] {
  override def zero = (agroup.zero, bgroup.zero, cgroup.zero, dgroup.zero, egroup.zero, fgroup.zero, ggroup.zero, hgroup.zero, igroup.zero, jgroup.zero, kgroup.zero)
  override def negate(v : (A, B, C, D, E, F, G, H, I, J, K)) = (agroup.negate(v._1), bgroup.negate(v._2), cgroup.negate(v._3), dgroup.negate(v._4), egroup.negate(v._5), fgroup.negate(v._6), ggroup.negate(v._7), hgroup.negate(v._8), igroup.negate(v._9), jgroup.negate(v._10), kgroup.negate(v._11))
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K), r : (A, B, C, D, E, F, G, H, I, J, K)) = (agroup.plus(l._1, r._1), bgroup.plus(l._2, r._2), cgroup.plus(l._3, r._3), dgroup.plus(l._4, r._4), egroup.plus(l._5, r._5), fgroup.plus(l._6, r._6), ggroup.plus(l._7, r._7), hgroup.plus(l._8, r._8), igroup.plus(l._9, r._9), jgroup.plus(l._10, r._10), kgroup.plus(l._11, r._11))
  override def minus(l : (A, B, C, D, E, F, G, H, I, J, K), r : (A, B, C, D, E, F, G, H, I, J, K)) = (agroup.minus(l._1, r._1), bgroup.minus(l._2, r._2), cgroup.minus(l._3, r._3), dgroup.minus(l._4, r._4), egroup.minus(l._5, r._5), fgroup.minus(l._6, r._6), ggroup.minus(l._7, r._7), hgroup.minus(l._8, r._8), igroup.minus(l._9, r._9), jgroup.minus(l._10, r._10), kgroup.minus(l._11, r._11))
}

/**
* Combine 11 rings into a product ring
*/
class Tuple11Ring[A, B, C, D, E, F, G, H, I, J, K](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I], jring : Ring[J], kring : Ring[K]) extends Ring[(A, B, C, D, E, F, G, H, I, J, K)] {
  override def zero = (aring.zero, bring.zero, cring.zero, dring.zero, ering.zero, fring.zero, gring.zero, hring.zero, iring.zero, jring.zero, kring.zero)
  override def one = (aring.one, bring.one, cring.one, dring.one, ering.one, fring.one, gring.one, hring.one, iring.one, jring.one, kring.one)
  override def negate(v : (A, B, C, D, E, F, G, H, I, J, K)) = (aring.negate(v._1), bring.negate(v._2), cring.negate(v._3), dring.negate(v._4), ering.negate(v._5), fring.negate(v._6), gring.negate(v._7), hring.negate(v._8), iring.negate(v._9), jring.negate(v._10), kring.negate(v._11))
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K), r : (A, B, C, D, E, F, G, H, I, J, K)) = (aring.plus(l._1, r._1), bring.plus(l._2, r._2), cring.plus(l._3, r._3), dring.plus(l._4, r._4), ering.plus(l._5, r._5), fring.plus(l._6, r._6), gring.plus(l._7, r._7), hring.plus(l._8, r._8), iring.plus(l._9, r._9), jring.plus(l._10, r._10), kring.plus(l._11, r._11))
  override def minus(l : (A, B, C, D, E, F, G, H, I, J, K), r : (A, B, C, D, E, F, G, H, I, J, K)) = (aring.minus(l._1, r._1), bring.minus(l._2, r._2), cring.minus(l._3, r._3), dring.minus(l._4, r._4), ering.minus(l._5, r._5), fring.minus(l._6, r._6), gring.minus(l._7, r._7), hring.minus(l._8, r._8), iring.minus(l._9, r._9), jring.minus(l._10, r._10), kring.minus(l._11, r._11))
  override def times(l : (A, B, C, D, E, F, G, H, I, J, K), r : (A, B, C, D, E, F, G, H, I, J, K)) = (aring.times(l._1, r._1), bring.times(l._2, r._2), cring.times(l._3, r._3), dring.times(l._4, r._4), ering.times(l._5, r._5), fring.times(l._6, r._6), gring.times(l._7, r._7), hring.times(l._8, r._8), iring.times(l._9, r._9), jring.times(l._10, r._10), kring.times(l._11, r._11))
}
/**
* Combine 12 monoids into a product monoid
*/
class Tuple12Monoid[A, B, C, D, E, F, G, H, I, J, K, L](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I], jmonoid : Monoid[J], kmonoid : Monoid[K], lmonoid : Monoid[L]) extends Monoid[(A, B, C, D, E, F, G, H, I, J, K, L)] {
  override def zero = (amonoid.zero, bmonoid.zero, cmonoid.zero, dmonoid.zero, emonoid.zero, fmonoid.zero, gmonoid.zero, hmonoid.zero, imonoid.zero, jmonoid.zero, kmonoid.zero, lmonoid.zero)
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L), r : (A, B, C, D, E, F, G, H, I, J, K, L)) = (amonoid.plus(l._1, r._1), bmonoid.plus(l._2, r._2), cmonoid.plus(l._3, r._3), dmonoid.plus(l._4, r._4), emonoid.plus(l._5, r._5), fmonoid.plus(l._6, r._6), gmonoid.plus(l._7, r._7), hmonoid.plus(l._8, r._8), imonoid.plus(l._9, r._9), jmonoid.plus(l._10, r._10), kmonoid.plus(l._11, r._11), lmonoid.plus(l._12, r._12))
}

/**
* Combine 12 groups into a product group
*/
class Tuple12Group[A, B, C, D, E, F, G, H, I, J, K, L](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I], jgroup : Group[J], kgroup : Group[K], lgroup : Group[L]) extends Group[(A, B, C, D, E, F, G, H, I, J, K, L)] {
  override def zero = (agroup.zero, bgroup.zero, cgroup.zero, dgroup.zero, egroup.zero, fgroup.zero, ggroup.zero, hgroup.zero, igroup.zero, jgroup.zero, kgroup.zero, lgroup.zero)
  override def negate(v : (A, B, C, D, E, F, G, H, I, J, K, L)) = (agroup.negate(v._1), bgroup.negate(v._2), cgroup.negate(v._3), dgroup.negate(v._4), egroup.negate(v._5), fgroup.negate(v._6), ggroup.negate(v._7), hgroup.negate(v._8), igroup.negate(v._9), jgroup.negate(v._10), kgroup.negate(v._11), lgroup.negate(v._12))
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L), r : (A, B, C, D, E, F, G, H, I, J, K, L)) = (agroup.plus(l._1, r._1), bgroup.plus(l._2, r._2), cgroup.plus(l._3, r._3), dgroup.plus(l._4, r._4), egroup.plus(l._5, r._5), fgroup.plus(l._6, r._6), ggroup.plus(l._7, r._7), hgroup.plus(l._8, r._8), igroup.plus(l._9, r._9), jgroup.plus(l._10, r._10), kgroup.plus(l._11, r._11), lgroup.plus(l._12, r._12))
  override def minus(l : (A, B, C, D, E, F, G, H, I, J, K, L), r : (A, B, C, D, E, F, G, H, I, J, K, L)) = (agroup.minus(l._1, r._1), bgroup.minus(l._2, r._2), cgroup.minus(l._3, r._3), dgroup.minus(l._4, r._4), egroup.minus(l._5, r._5), fgroup.minus(l._6, r._6), ggroup.minus(l._7, r._7), hgroup.minus(l._8, r._8), igroup.minus(l._9, r._9), jgroup.minus(l._10, r._10), kgroup.minus(l._11, r._11), lgroup.minus(l._12, r._12))
}

/**
* Combine 12 rings into a product ring
*/
class Tuple12Ring[A, B, C, D, E, F, G, H, I, J, K, L](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I], jring : Ring[J], kring : Ring[K], lring : Ring[L]) extends Ring[(A, B, C, D, E, F, G, H, I, J, K, L)] {
  override def zero = (aring.zero, bring.zero, cring.zero, dring.zero, ering.zero, fring.zero, gring.zero, hring.zero, iring.zero, jring.zero, kring.zero, lring.zero)
  override def one = (aring.one, bring.one, cring.one, dring.one, ering.one, fring.one, gring.one, hring.one, iring.one, jring.one, kring.one, lring.one)
  override def negate(v : (A, B, C, D, E, F, G, H, I, J, K, L)) = (aring.negate(v._1), bring.negate(v._2), cring.negate(v._3), dring.negate(v._4), ering.negate(v._5), fring.negate(v._6), gring.negate(v._7), hring.negate(v._8), iring.negate(v._9), jring.negate(v._10), kring.negate(v._11), lring.negate(v._12))
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L), r : (A, B, C, D, E, F, G, H, I, J, K, L)) = (aring.plus(l._1, r._1), bring.plus(l._2, r._2), cring.plus(l._3, r._3), dring.plus(l._4, r._4), ering.plus(l._5, r._5), fring.plus(l._6, r._6), gring.plus(l._7, r._7), hring.plus(l._8, r._8), iring.plus(l._9, r._9), jring.plus(l._10, r._10), kring.plus(l._11, r._11), lring.plus(l._12, r._12))
  override def minus(l : (A, B, C, D, E, F, G, H, I, J, K, L), r : (A, B, C, D, E, F, G, H, I, J, K, L)) = (aring.minus(l._1, r._1), bring.minus(l._2, r._2), cring.minus(l._3, r._3), dring.minus(l._4, r._4), ering.minus(l._5, r._5), fring.minus(l._6, r._6), gring.minus(l._7, r._7), hring.minus(l._8, r._8), iring.minus(l._9, r._9), jring.minus(l._10, r._10), kring.minus(l._11, r._11), lring.minus(l._12, r._12))
  override def times(l : (A, B, C, D, E, F, G, H, I, J, K, L), r : (A, B, C, D, E, F, G, H, I, J, K, L)) = (aring.times(l._1, r._1), bring.times(l._2, r._2), cring.times(l._3, r._3), dring.times(l._4, r._4), ering.times(l._5, r._5), fring.times(l._6, r._6), gring.times(l._7, r._7), hring.times(l._8, r._8), iring.times(l._9, r._9), jring.times(l._10, r._10), kring.times(l._11, r._11), lring.times(l._12, r._12))
}
/**
* Combine 13 monoids into a product monoid
*/
class Tuple13Monoid[A, B, C, D, E, F, G, H, I, J, K, L, M](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I], jmonoid : Monoid[J], kmonoid : Monoid[K], lmonoid : Monoid[L], mmonoid : Monoid[M]) extends Monoid[(A, B, C, D, E, F, G, H, I, J, K, L, M)] {
  override def zero = (amonoid.zero, bmonoid.zero, cmonoid.zero, dmonoid.zero, emonoid.zero, fmonoid.zero, gmonoid.zero, hmonoid.zero, imonoid.zero, jmonoid.zero, kmonoid.zero, lmonoid.zero, mmonoid.zero)
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M), r : (A, B, C, D, E, F, G, H, I, J, K, L, M)) = (amonoid.plus(l._1, r._1), bmonoid.plus(l._2, r._2), cmonoid.plus(l._3, r._3), dmonoid.plus(l._4, r._4), emonoid.plus(l._5, r._5), fmonoid.plus(l._6, r._6), gmonoid.plus(l._7, r._7), hmonoid.plus(l._8, r._8), imonoid.plus(l._9, r._9), jmonoid.plus(l._10, r._10), kmonoid.plus(l._11, r._11), lmonoid.plus(l._12, r._12), mmonoid.plus(l._13, r._13))
}

/**
* Combine 13 groups into a product group
*/
class Tuple13Group[A, B, C, D, E, F, G, H, I, J, K, L, M](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I], jgroup : Group[J], kgroup : Group[K], lgroup : Group[L], mgroup : Group[M]) extends Group[(A, B, C, D, E, F, G, H, I, J, K, L, M)] {
  override def zero = (agroup.zero, bgroup.zero, cgroup.zero, dgroup.zero, egroup.zero, fgroup.zero, ggroup.zero, hgroup.zero, igroup.zero, jgroup.zero, kgroup.zero, lgroup.zero, mgroup.zero)
  override def negate(v : (A, B, C, D, E, F, G, H, I, J, K, L, M)) = (agroup.negate(v._1), bgroup.negate(v._2), cgroup.negate(v._3), dgroup.negate(v._4), egroup.negate(v._5), fgroup.negate(v._6), ggroup.negate(v._7), hgroup.negate(v._8), igroup.negate(v._9), jgroup.negate(v._10), kgroup.negate(v._11), lgroup.negate(v._12), mgroup.negate(v._13))
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M), r : (A, B, C, D, E, F, G, H, I, J, K, L, M)) = (agroup.plus(l._1, r._1), bgroup.plus(l._2, r._2), cgroup.plus(l._3, r._3), dgroup.plus(l._4, r._4), egroup.plus(l._5, r._5), fgroup.plus(l._6, r._6), ggroup.plus(l._7, r._7), hgroup.plus(l._8, r._8), igroup.plus(l._9, r._9), jgroup.plus(l._10, r._10), kgroup.plus(l._11, r._11), lgroup.plus(l._12, r._12), mgroup.plus(l._13, r._13))
  override def minus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M), r : (A, B, C, D, E, F, G, H, I, J, K, L, M)) = (agroup.minus(l._1, r._1), bgroup.minus(l._2, r._2), cgroup.minus(l._3, r._3), dgroup.minus(l._4, r._4), egroup.minus(l._5, r._5), fgroup.minus(l._6, r._6), ggroup.minus(l._7, r._7), hgroup.minus(l._8, r._8), igroup.minus(l._9, r._9), jgroup.minus(l._10, r._10), kgroup.minus(l._11, r._11), lgroup.minus(l._12, r._12), mgroup.minus(l._13, r._13))
}

/**
* Combine 13 rings into a product ring
*/
class Tuple13Ring[A, B, C, D, E, F, G, H, I, J, K, L, M](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I], jring : Ring[J], kring : Ring[K], lring : Ring[L], mring : Ring[M]) extends Ring[(A, B, C, D, E, F, G, H, I, J, K, L, M)] {
  override def zero = (aring.zero, bring.zero, cring.zero, dring.zero, ering.zero, fring.zero, gring.zero, hring.zero, iring.zero, jring.zero, kring.zero, lring.zero, mring.zero)
  override def one = (aring.one, bring.one, cring.one, dring.one, ering.one, fring.one, gring.one, hring.one, iring.one, jring.one, kring.one, lring.one, mring.one)
  override def negate(v : (A, B, C, D, E, F, G, H, I, J, K, L, M)) = (aring.negate(v._1), bring.negate(v._2), cring.negate(v._3), dring.negate(v._4), ering.negate(v._5), fring.negate(v._6), gring.negate(v._7), hring.negate(v._8), iring.negate(v._9), jring.negate(v._10), kring.negate(v._11), lring.negate(v._12), mring.negate(v._13))
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M), r : (A, B, C, D, E, F, G, H, I, J, K, L, M)) = (aring.plus(l._1, r._1), bring.plus(l._2, r._2), cring.plus(l._3, r._3), dring.plus(l._4, r._4), ering.plus(l._5, r._5), fring.plus(l._6, r._6), gring.plus(l._7, r._7), hring.plus(l._8, r._8), iring.plus(l._9, r._9), jring.plus(l._10, r._10), kring.plus(l._11, r._11), lring.plus(l._12, r._12), mring.plus(l._13, r._13))
  override def minus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M), r : (A, B, C, D, E, F, G, H, I, J, K, L, M)) = (aring.minus(l._1, r._1), bring.minus(l._2, r._2), cring.minus(l._3, r._3), dring.minus(l._4, r._4), ering.minus(l._5, r._5), fring.minus(l._6, r._6), gring.minus(l._7, r._7), hring.minus(l._8, r._8), iring.minus(l._9, r._9), jring.minus(l._10, r._10), kring.minus(l._11, r._11), lring.minus(l._12, r._12), mring.minus(l._13, r._13))
  override def times(l : (A, B, C, D, E, F, G, H, I, J, K, L, M), r : (A, B, C, D, E, F, G, H, I, J, K, L, M)) = (aring.times(l._1, r._1), bring.times(l._2, r._2), cring.times(l._3, r._3), dring.times(l._4, r._4), ering.times(l._5, r._5), fring.times(l._6, r._6), gring.times(l._7, r._7), hring.times(l._8, r._8), iring.times(l._9, r._9), jring.times(l._10, r._10), kring.times(l._11, r._11), lring.times(l._12, r._12), mring.times(l._13, r._13))
}
/**
* Combine 14 monoids into a product monoid
*/
class Tuple14Monoid[A, B, C, D, E, F, G, H, I, J, K, L, M, N](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I], jmonoid : Monoid[J], kmonoid : Monoid[K], lmonoid : Monoid[L], mmonoid : Monoid[M], nmonoid : Monoid[N]) extends Monoid[(A, B, C, D, E, F, G, H, I, J, K, L, M, N)] {
  override def zero = (amonoid.zero, bmonoid.zero, cmonoid.zero, dmonoid.zero, emonoid.zero, fmonoid.zero, gmonoid.zero, hmonoid.zero, imonoid.zero, jmonoid.zero, kmonoid.zero, lmonoid.zero, mmonoid.zero, nmonoid.zero)
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N)) = (amonoid.plus(l._1, r._1), bmonoid.plus(l._2, r._2), cmonoid.plus(l._3, r._3), dmonoid.plus(l._4, r._4), emonoid.plus(l._5, r._5), fmonoid.plus(l._6, r._6), gmonoid.plus(l._7, r._7), hmonoid.plus(l._8, r._8), imonoid.plus(l._9, r._9), jmonoid.plus(l._10, r._10), kmonoid.plus(l._11, r._11), lmonoid.plus(l._12, r._12), mmonoid.plus(l._13, r._13), nmonoid.plus(l._14, r._14))
}

/**
* Combine 14 groups into a product group
*/
class Tuple14Group[A, B, C, D, E, F, G, H, I, J, K, L, M, N](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I], jgroup : Group[J], kgroup : Group[K], lgroup : Group[L], mgroup : Group[M], ngroup : Group[N]) extends Group[(A, B, C, D, E, F, G, H, I, J, K, L, M, N)] {
  override def zero = (agroup.zero, bgroup.zero, cgroup.zero, dgroup.zero, egroup.zero, fgroup.zero, ggroup.zero, hgroup.zero, igroup.zero, jgroup.zero, kgroup.zero, lgroup.zero, mgroup.zero, ngroup.zero)
  override def negate(v : (A, B, C, D, E, F, G, H, I, J, K, L, M, N)) = (agroup.negate(v._1), bgroup.negate(v._2), cgroup.negate(v._3), dgroup.negate(v._4), egroup.negate(v._5), fgroup.negate(v._6), ggroup.negate(v._7), hgroup.negate(v._8), igroup.negate(v._9), jgroup.negate(v._10), kgroup.negate(v._11), lgroup.negate(v._12), mgroup.negate(v._13), ngroup.negate(v._14))
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N)) = (agroup.plus(l._1, r._1), bgroup.plus(l._2, r._2), cgroup.plus(l._3, r._3), dgroup.plus(l._4, r._4), egroup.plus(l._5, r._5), fgroup.plus(l._6, r._6), ggroup.plus(l._7, r._7), hgroup.plus(l._8, r._8), igroup.plus(l._9, r._9), jgroup.plus(l._10, r._10), kgroup.plus(l._11, r._11), lgroup.plus(l._12, r._12), mgroup.plus(l._13, r._13), ngroup.plus(l._14, r._14))
  override def minus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N)) = (agroup.minus(l._1, r._1), bgroup.minus(l._2, r._2), cgroup.minus(l._3, r._3), dgroup.minus(l._4, r._4), egroup.minus(l._5, r._5), fgroup.minus(l._6, r._6), ggroup.minus(l._7, r._7), hgroup.minus(l._8, r._8), igroup.minus(l._9, r._9), jgroup.minus(l._10, r._10), kgroup.minus(l._11, r._11), lgroup.minus(l._12, r._12), mgroup.minus(l._13, r._13), ngroup.minus(l._14, r._14))
}

/**
* Combine 14 rings into a product ring
*/
class Tuple14Ring[A, B, C, D, E, F, G, H, I, J, K, L, M, N](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I], jring : Ring[J], kring : Ring[K], lring : Ring[L], mring : Ring[M], nring : Ring[N]) extends Ring[(A, B, C, D, E, F, G, H, I, J, K, L, M, N)] {
  override def zero = (aring.zero, bring.zero, cring.zero, dring.zero, ering.zero, fring.zero, gring.zero, hring.zero, iring.zero, jring.zero, kring.zero, lring.zero, mring.zero, nring.zero)
  override def one = (aring.one, bring.one, cring.one, dring.one, ering.one, fring.one, gring.one, hring.one, iring.one, jring.one, kring.one, lring.one, mring.one, nring.one)
  override def negate(v : (A, B, C, D, E, F, G, H, I, J, K, L, M, N)) = (aring.negate(v._1), bring.negate(v._2), cring.negate(v._3), dring.negate(v._4), ering.negate(v._5), fring.negate(v._6), gring.negate(v._7), hring.negate(v._8), iring.negate(v._9), jring.negate(v._10), kring.negate(v._11), lring.negate(v._12), mring.negate(v._13), nring.negate(v._14))
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N)) = (aring.plus(l._1, r._1), bring.plus(l._2, r._2), cring.plus(l._3, r._3), dring.plus(l._4, r._4), ering.plus(l._5, r._5), fring.plus(l._6, r._6), gring.plus(l._7, r._7), hring.plus(l._8, r._8), iring.plus(l._9, r._9), jring.plus(l._10, r._10), kring.plus(l._11, r._11), lring.plus(l._12, r._12), mring.plus(l._13, r._13), nring.plus(l._14, r._14))
  override def minus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N)) = (aring.minus(l._1, r._1), bring.minus(l._2, r._2), cring.minus(l._3, r._3), dring.minus(l._4, r._4), ering.minus(l._5, r._5), fring.minus(l._6, r._6), gring.minus(l._7, r._7), hring.minus(l._8, r._8), iring.minus(l._9, r._9), jring.minus(l._10, r._10), kring.minus(l._11, r._11), lring.minus(l._12, r._12), mring.minus(l._13, r._13), nring.minus(l._14, r._14))
  override def times(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N)) = (aring.times(l._1, r._1), bring.times(l._2, r._2), cring.times(l._3, r._3), dring.times(l._4, r._4), ering.times(l._5, r._5), fring.times(l._6, r._6), gring.times(l._7, r._7), hring.times(l._8, r._8), iring.times(l._9, r._9), jring.times(l._10, r._10), kring.times(l._11, r._11), lring.times(l._12, r._12), mring.times(l._13, r._13), nring.times(l._14, r._14))
}
/**
* Combine 15 monoids into a product monoid
*/
class Tuple15Monoid[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I], jmonoid : Monoid[J], kmonoid : Monoid[K], lmonoid : Monoid[L], mmonoid : Monoid[M], nmonoid : Monoid[N], omonoid : Monoid[O]) extends Monoid[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)] {
  override def zero = (amonoid.zero, bmonoid.zero, cmonoid.zero, dmonoid.zero, emonoid.zero, fmonoid.zero, gmonoid.zero, hmonoid.zero, imonoid.zero, jmonoid.zero, kmonoid.zero, lmonoid.zero, mmonoid.zero, nmonoid.zero, omonoid.zero)
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)) = (amonoid.plus(l._1, r._1), bmonoid.plus(l._2, r._2), cmonoid.plus(l._3, r._3), dmonoid.plus(l._4, r._4), emonoid.plus(l._5, r._5), fmonoid.plus(l._6, r._6), gmonoid.plus(l._7, r._7), hmonoid.plus(l._8, r._8), imonoid.plus(l._9, r._9), jmonoid.plus(l._10, r._10), kmonoid.plus(l._11, r._11), lmonoid.plus(l._12, r._12), mmonoid.plus(l._13, r._13), nmonoid.plus(l._14, r._14), omonoid.plus(l._15, r._15))
}

/**
* Combine 15 groups into a product group
*/
class Tuple15Group[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I], jgroup : Group[J], kgroup : Group[K], lgroup : Group[L], mgroup : Group[M], ngroup : Group[N], ogroup : Group[O]) extends Group[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)] {
  override def zero = (agroup.zero, bgroup.zero, cgroup.zero, dgroup.zero, egroup.zero, fgroup.zero, ggroup.zero, hgroup.zero, igroup.zero, jgroup.zero, kgroup.zero, lgroup.zero, mgroup.zero, ngroup.zero, ogroup.zero)
  override def negate(v : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)) = (agroup.negate(v._1), bgroup.negate(v._2), cgroup.negate(v._3), dgroup.negate(v._4), egroup.negate(v._5), fgroup.negate(v._6), ggroup.negate(v._7), hgroup.negate(v._8), igroup.negate(v._9), jgroup.negate(v._10), kgroup.negate(v._11), lgroup.negate(v._12), mgroup.negate(v._13), ngroup.negate(v._14), ogroup.negate(v._15))
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)) = (agroup.plus(l._1, r._1), bgroup.plus(l._2, r._2), cgroup.plus(l._3, r._3), dgroup.plus(l._4, r._4), egroup.plus(l._5, r._5), fgroup.plus(l._6, r._6), ggroup.plus(l._7, r._7), hgroup.plus(l._8, r._8), igroup.plus(l._9, r._9), jgroup.plus(l._10, r._10), kgroup.plus(l._11, r._11), lgroup.plus(l._12, r._12), mgroup.plus(l._13, r._13), ngroup.plus(l._14, r._14), ogroup.plus(l._15, r._15))
  override def minus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)) = (agroup.minus(l._1, r._1), bgroup.minus(l._2, r._2), cgroup.minus(l._3, r._3), dgroup.minus(l._4, r._4), egroup.minus(l._5, r._5), fgroup.minus(l._6, r._6), ggroup.minus(l._7, r._7), hgroup.minus(l._8, r._8), igroup.minus(l._9, r._9), jgroup.minus(l._10, r._10), kgroup.minus(l._11, r._11), lgroup.minus(l._12, r._12), mgroup.minus(l._13, r._13), ngroup.minus(l._14, r._14), ogroup.minus(l._15, r._15))
}

/**
* Combine 15 rings into a product ring
*/
class Tuple15Ring[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I], jring : Ring[J], kring : Ring[K], lring : Ring[L], mring : Ring[M], nring : Ring[N], oring : Ring[O]) extends Ring[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)] {
  override def zero = (aring.zero, bring.zero, cring.zero, dring.zero, ering.zero, fring.zero, gring.zero, hring.zero, iring.zero, jring.zero, kring.zero, lring.zero, mring.zero, nring.zero, oring.zero)
  override def one = (aring.one, bring.one, cring.one, dring.one, ering.one, fring.one, gring.one, hring.one, iring.one, jring.one, kring.one, lring.one, mring.one, nring.one, oring.one)
  override def negate(v : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)) = (aring.negate(v._1), bring.negate(v._2), cring.negate(v._3), dring.negate(v._4), ering.negate(v._5), fring.negate(v._6), gring.negate(v._7), hring.negate(v._8), iring.negate(v._9), jring.negate(v._10), kring.negate(v._11), lring.negate(v._12), mring.negate(v._13), nring.negate(v._14), oring.negate(v._15))
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)) = (aring.plus(l._1, r._1), bring.plus(l._2, r._2), cring.plus(l._3, r._3), dring.plus(l._4, r._4), ering.plus(l._5, r._5), fring.plus(l._6, r._6), gring.plus(l._7, r._7), hring.plus(l._8, r._8), iring.plus(l._9, r._9), jring.plus(l._10, r._10), kring.plus(l._11, r._11), lring.plus(l._12, r._12), mring.plus(l._13, r._13), nring.plus(l._14, r._14), oring.plus(l._15, r._15))
  override def minus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)) = (aring.minus(l._1, r._1), bring.minus(l._2, r._2), cring.minus(l._3, r._3), dring.minus(l._4, r._4), ering.minus(l._5, r._5), fring.minus(l._6, r._6), gring.minus(l._7, r._7), hring.minus(l._8, r._8), iring.minus(l._9, r._9), jring.minus(l._10, r._10), kring.minus(l._11, r._11), lring.minus(l._12, r._12), mring.minus(l._13, r._13), nring.minus(l._14, r._14), oring.minus(l._15, r._15))
  override def times(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)) = (aring.times(l._1, r._1), bring.times(l._2, r._2), cring.times(l._3, r._3), dring.times(l._4, r._4), ering.times(l._5, r._5), fring.times(l._6, r._6), gring.times(l._7, r._7), hring.times(l._8, r._8), iring.times(l._9, r._9), jring.times(l._10, r._10), kring.times(l._11, r._11), lring.times(l._12, r._12), mring.times(l._13, r._13), nring.times(l._14, r._14), oring.times(l._15, r._15))
}
/**
* Combine 16 monoids into a product monoid
*/
class Tuple16Monoid[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I], jmonoid : Monoid[J], kmonoid : Monoid[K], lmonoid : Monoid[L], mmonoid : Monoid[M], nmonoid : Monoid[N], omonoid : Monoid[O], pmonoid : Monoid[P]) extends Monoid[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)] {
  override def zero = (amonoid.zero, bmonoid.zero, cmonoid.zero, dmonoid.zero, emonoid.zero, fmonoid.zero, gmonoid.zero, hmonoid.zero, imonoid.zero, jmonoid.zero, kmonoid.zero, lmonoid.zero, mmonoid.zero, nmonoid.zero, omonoid.zero, pmonoid.zero)
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)) = (amonoid.plus(l._1, r._1), bmonoid.plus(l._2, r._2), cmonoid.plus(l._3, r._3), dmonoid.plus(l._4, r._4), emonoid.plus(l._5, r._5), fmonoid.plus(l._6, r._6), gmonoid.plus(l._7, r._7), hmonoid.plus(l._8, r._8), imonoid.plus(l._9, r._9), jmonoid.plus(l._10, r._10), kmonoid.plus(l._11, r._11), lmonoid.plus(l._12, r._12), mmonoid.plus(l._13, r._13), nmonoid.plus(l._14, r._14), omonoid.plus(l._15, r._15), pmonoid.plus(l._16, r._16))
}

/**
* Combine 16 groups into a product group
*/
class Tuple16Group[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I], jgroup : Group[J], kgroup : Group[K], lgroup : Group[L], mgroup : Group[M], ngroup : Group[N], ogroup : Group[O], pgroup : Group[P]) extends Group[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)] {
  override def zero = (agroup.zero, bgroup.zero, cgroup.zero, dgroup.zero, egroup.zero, fgroup.zero, ggroup.zero, hgroup.zero, igroup.zero, jgroup.zero, kgroup.zero, lgroup.zero, mgroup.zero, ngroup.zero, ogroup.zero, pgroup.zero)
  override def negate(v : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)) = (agroup.negate(v._1), bgroup.negate(v._2), cgroup.negate(v._3), dgroup.negate(v._4), egroup.negate(v._5), fgroup.negate(v._6), ggroup.negate(v._7), hgroup.negate(v._8), igroup.negate(v._9), jgroup.negate(v._10), kgroup.negate(v._11), lgroup.negate(v._12), mgroup.negate(v._13), ngroup.negate(v._14), ogroup.negate(v._15), pgroup.negate(v._16))
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)) = (agroup.plus(l._1, r._1), bgroup.plus(l._2, r._2), cgroup.plus(l._3, r._3), dgroup.plus(l._4, r._4), egroup.plus(l._5, r._5), fgroup.plus(l._6, r._6), ggroup.plus(l._7, r._7), hgroup.plus(l._8, r._8), igroup.plus(l._9, r._9), jgroup.plus(l._10, r._10), kgroup.plus(l._11, r._11), lgroup.plus(l._12, r._12), mgroup.plus(l._13, r._13), ngroup.plus(l._14, r._14), ogroup.plus(l._15, r._15), pgroup.plus(l._16, r._16))
  override def minus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)) = (agroup.minus(l._1, r._1), bgroup.minus(l._2, r._2), cgroup.minus(l._3, r._3), dgroup.minus(l._4, r._4), egroup.minus(l._5, r._5), fgroup.minus(l._6, r._6), ggroup.minus(l._7, r._7), hgroup.minus(l._8, r._8), igroup.minus(l._9, r._9), jgroup.minus(l._10, r._10), kgroup.minus(l._11, r._11), lgroup.minus(l._12, r._12), mgroup.minus(l._13, r._13), ngroup.minus(l._14, r._14), ogroup.minus(l._15, r._15), pgroup.minus(l._16, r._16))
}

/**
* Combine 16 rings into a product ring
*/
class Tuple16Ring[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I], jring : Ring[J], kring : Ring[K], lring : Ring[L], mring : Ring[M], nring : Ring[N], oring : Ring[O], pring : Ring[P]) extends Ring[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)] {
  override def zero = (aring.zero, bring.zero, cring.zero, dring.zero, ering.zero, fring.zero, gring.zero, hring.zero, iring.zero, jring.zero, kring.zero, lring.zero, mring.zero, nring.zero, oring.zero, pring.zero)
  override def one = (aring.one, bring.one, cring.one, dring.one, ering.one, fring.one, gring.one, hring.one, iring.one, jring.one, kring.one, lring.one, mring.one, nring.one, oring.one, pring.one)
  override def negate(v : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)) = (aring.negate(v._1), bring.negate(v._2), cring.negate(v._3), dring.negate(v._4), ering.negate(v._5), fring.negate(v._6), gring.negate(v._7), hring.negate(v._8), iring.negate(v._9), jring.negate(v._10), kring.negate(v._11), lring.negate(v._12), mring.negate(v._13), nring.negate(v._14), oring.negate(v._15), pring.negate(v._16))
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)) = (aring.plus(l._1, r._1), bring.plus(l._2, r._2), cring.plus(l._3, r._3), dring.plus(l._4, r._4), ering.plus(l._5, r._5), fring.plus(l._6, r._6), gring.plus(l._7, r._7), hring.plus(l._8, r._8), iring.plus(l._9, r._9), jring.plus(l._10, r._10), kring.plus(l._11, r._11), lring.plus(l._12, r._12), mring.plus(l._13, r._13), nring.plus(l._14, r._14), oring.plus(l._15, r._15), pring.plus(l._16, r._16))
  override def minus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)) = (aring.minus(l._1, r._1), bring.minus(l._2, r._2), cring.minus(l._3, r._3), dring.minus(l._4, r._4), ering.minus(l._5, r._5), fring.minus(l._6, r._6), gring.minus(l._7, r._7), hring.minus(l._8, r._8), iring.minus(l._9, r._9), jring.minus(l._10, r._10), kring.minus(l._11, r._11), lring.minus(l._12, r._12), mring.minus(l._13, r._13), nring.minus(l._14, r._14), oring.minus(l._15, r._15), pring.minus(l._16, r._16))
  override def times(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)) = (aring.times(l._1, r._1), bring.times(l._2, r._2), cring.times(l._3, r._3), dring.times(l._4, r._4), ering.times(l._5, r._5), fring.times(l._6, r._6), gring.times(l._7, r._7), hring.times(l._8, r._8), iring.times(l._9, r._9), jring.times(l._10, r._10), kring.times(l._11, r._11), lring.times(l._12, r._12), mring.times(l._13, r._13), nring.times(l._14, r._14), oring.times(l._15, r._15), pring.times(l._16, r._16))
}
/**
* Combine 17 monoids into a product monoid
*/
class Tuple17Monoid[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I], jmonoid : Monoid[J], kmonoid : Monoid[K], lmonoid : Monoid[L], mmonoid : Monoid[M], nmonoid : Monoid[N], omonoid : Monoid[O], pmonoid : Monoid[P], qmonoid : Monoid[Q]) extends Monoid[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)] {
  override def zero = (amonoid.zero, bmonoid.zero, cmonoid.zero, dmonoid.zero, emonoid.zero, fmonoid.zero, gmonoid.zero, hmonoid.zero, imonoid.zero, jmonoid.zero, kmonoid.zero, lmonoid.zero, mmonoid.zero, nmonoid.zero, omonoid.zero, pmonoid.zero, qmonoid.zero)
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)) = (amonoid.plus(l._1, r._1), bmonoid.plus(l._2, r._2), cmonoid.plus(l._3, r._3), dmonoid.plus(l._4, r._4), emonoid.plus(l._5, r._5), fmonoid.plus(l._6, r._6), gmonoid.plus(l._7, r._7), hmonoid.plus(l._8, r._8), imonoid.plus(l._9, r._9), jmonoid.plus(l._10, r._10), kmonoid.plus(l._11, r._11), lmonoid.plus(l._12, r._12), mmonoid.plus(l._13, r._13), nmonoid.plus(l._14, r._14), omonoid.plus(l._15, r._15), pmonoid.plus(l._16, r._16), qmonoid.plus(l._17, r._17))
}

/**
* Combine 17 groups into a product group
*/
class Tuple17Group[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I], jgroup : Group[J], kgroup : Group[K], lgroup : Group[L], mgroup : Group[M], ngroup : Group[N], ogroup : Group[O], pgroup : Group[P], qgroup : Group[Q]) extends Group[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)] {
  override def zero = (agroup.zero, bgroup.zero, cgroup.zero, dgroup.zero, egroup.zero, fgroup.zero, ggroup.zero, hgroup.zero, igroup.zero, jgroup.zero, kgroup.zero, lgroup.zero, mgroup.zero, ngroup.zero, ogroup.zero, pgroup.zero, qgroup.zero)
  override def negate(v : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)) = (agroup.negate(v._1), bgroup.negate(v._2), cgroup.negate(v._3), dgroup.negate(v._4), egroup.negate(v._5), fgroup.negate(v._6), ggroup.negate(v._7), hgroup.negate(v._8), igroup.negate(v._9), jgroup.negate(v._10), kgroup.negate(v._11), lgroup.negate(v._12), mgroup.negate(v._13), ngroup.negate(v._14), ogroup.negate(v._15), pgroup.negate(v._16), qgroup.negate(v._17))
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)) = (agroup.plus(l._1, r._1), bgroup.plus(l._2, r._2), cgroup.plus(l._3, r._3), dgroup.plus(l._4, r._4), egroup.plus(l._5, r._5), fgroup.plus(l._6, r._6), ggroup.plus(l._7, r._7), hgroup.plus(l._8, r._8), igroup.plus(l._9, r._9), jgroup.plus(l._10, r._10), kgroup.plus(l._11, r._11), lgroup.plus(l._12, r._12), mgroup.plus(l._13, r._13), ngroup.plus(l._14, r._14), ogroup.plus(l._15, r._15), pgroup.plus(l._16, r._16), qgroup.plus(l._17, r._17))
  override def minus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)) = (agroup.minus(l._1, r._1), bgroup.minus(l._2, r._2), cgroup.minus(l._3, r._3), dgroup.minus(l._4, r._4), egroup.minus(l._5, r._5), fgroup.minus(l._6, r._6), ggroup.minus(l._7, r._7), hgroup.minus(l._8, r._8), igroup.minus(l._9, r._9), jgroup.minus(l._10, r._10), kgroup.minus(l._11, r._11), lgroup.minus(l._12, r._12), mgroup.minus(l._13, r._13), ngroup.minus(l._14, r._14), ogroup.minus(l._15, r._15), pgroup.minus(l._16, r._16), qgroup.minus(l._17, r._17))
}

/**
* Combine 17 rings into a product ring
*/
class Tuple17Ring[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I], jring : Ring[J], kring : Ring[K], lring : Ring[L], mring : Ring[M], nring : Ring[N], oring : Ring[O], pring : Ring[P], qring : Ring[Q]) extends Ring[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)] {
  override def zero = (aring.zero, bring.zero, cring.zero, dring.zero, ering.zero, fring.zero, gring.zero, hring.zero, iring.zero, jring.zero, kring.zero, lring.zero, mring.zero, nring.zero, oring.zero, pring.zero, qring.zero)
  override def one = (aring.one, bring.one, cring.one, dring.one, ering.one, fring.one, gring.one, hring.one, iring.one, jring.one, kring.one, lring.one, mring.one, nring.one, oring.one, pring.one, qring.one)
  override def negate(v : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)) = (aring.negate(v._1), bring.negate(v._2), cring.negate(v._3), dring.negate(v._4), ering.negate(v._5), fring.negate(v._6), gring.negate(v._7), hring.negate(v._8), iring.negate(v._9), jring.negate(v._10), kring.negate(v._11), lring.negate(v._12), mring.negate(v._13), nring.negate(v._14), oring.negate(v._15), pring.negate(v._16), qring.negate(v._17))
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)) = (aring.plus(l._1, r._1), bring.plus(l._2, r._2), cring.plus(l._3, r._3), dring.plus(l._4, r._4), ering.plus(l._5, r._5), fring.plus(l._6, r._6), gring.plus(l._7, r._7), hring.plus(l._8, r._8), iring.plus(l._9, r._9), jring.plus(l._10, r._10), kring.plus(l._11, r._11), lring.plus(l._12, r._12), mring.plus(l._13, r._13), nring.plus(l._14, r._14), oring.plus(l._15, r._15), pring.plus(l._16, r._16), qring.plus(l._17, r._17))
  override def minus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)) = (aring.minus(l._1, r._1), bring.minus(l._2, r._2), cring.minus(l._3, r._3), dring.minus(l._4, r._4), ering.minus(l._5, r._5), fring.minus(l._6, r._6), gring.minus(l._7, r._7), hring.minus(l._8, r._8), iring.minus(l._9, r._9), jring.minus(l._10, r._10), kring.minus(l._11, r._11), lring.minus(l._12, r._12), mring.minus(l._13, r._13), nring.minus(l._14, r._14), oring.minus(l._15, r._15), pring.minus(l._16, r._16), qring.minus(l._17, r._17))
  override def times(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)) = (aring.times(l._1, r._1), bring.times(l._2, r._2), cring.times(l._3, r._3), dring.times(l._4, r._4), ering.times(l._5, r._5), fring.times(l._6, r._6), gring.times(l._7, r._7), hring.times(l._8, r._8), iring.times(l._9, r._9), jring.times(l._10, r._10), kring.times(l._11, r._11), lring.times(l._12, r._12), mring.times(l._13, r._13), nring.times(l._14, r._14), oring.times(l._15, r._15), pring.times(l._16, r._16), qring.times(l._17, r._17))
}
/**
* Combine 18 monoids into a product monoid
*/
class Tuple18Monoid[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I], jmonoid : Monoid[J], kmonoid : Monoid[K], lmonoid : Monoid[L], mmonoid : Monoid[M], nmonoid : Monoid[N], omonoid : Monoid[O], pmonoid : Monoid[P], qmonoid : Monoid[Q], rmonoid : Monoid[R]) extends Monoid[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)] {
  override def zero = (amonoid.zero, bmonoid.zero, cmonoid.zero, dmonoid.zero, emonoid.zero, fmonoid.zero, gmonoid.zero, hmonoid.zero, imonoid.zero, jmonoid.zero, kmonoid.zero, lmonoid.zero, mmonoid.zero, nmonoid.zero, omonoid.zero, pmonoid.zero, qmonoid.zero, rmonoid.zero)
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)) = (amonoid.plus(l._1, r._1), bmonoid.plus(l._2, r._2), cmonoid.plus(l._3, r._3), dmonoid.plus(l._4, r._4), emonoid.plus(l._5, r._5), fmonoid.plus(l._6, r._6), gmonoid.plus(l._7, r._7), hmonoid.plus(l._8, r._8), imonoid.plus(l._9, r._9), jmonoid.plus(l._10, r._10), kmonoid.plus(l._11, r._11), lmonoid.plus(l._12, r._12), mmonoid.plus(l._13, r._13), nmonoid.plus(l._14, r._14), omonoid.plus(l._15, r._15), pmonoid.plus(l._16, r._16), qmonoid.plus(l._17, r._17), rmonoid.plus(l._18, r._18))
}

/**
* Combine 18 groups into a product group
*/
class Tuple18Group[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I], jgroup : Group[J], kgroup : Group[K], lgroup : Group[L], mgroup : Group[M], ngroup : Group[N], ogroup : Group[O], pgroup : Group[P], qgroup : Group[Q], rgroup : Group[R]) extends Group[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)] {
  override def zero = (agroup.zero, bgroup.zero, cgroup.zero, dgroup.zero, egroup.zero, fgroup.zero, ggroup.zero, hgroup.zero, igroup.zero, jgroup.zero, kgroup.zero, lgroup.zero, mgroup.zero, ngroup.zero, ogroup.zero, pgroup.zero, qgroup.zero, rgroup.zero)
  override def negate(v : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)) = (agroup.negate(v._1), bgroup.negate(v._2), cgroup.negate(v._3), dgroup.negate(v._4), egroup.negate(v._5), fgroup.negate(v._6), ggroup.negate(v._7), hgroup.negate(v._8), igroup.negate(v._9), jgroup.negate(v._10), kgroup.negate(v._11), lgroup.negate(v._12), mgroup.negate(v._13), ngroup.negate(v._14), ogroup.negate(v._15), pgroup.negate(v._16), qgroup.negate(v._17), rgroup.negate(v._18))
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)) = (agroup.plus(l._1, r._1), bgroup.plus(l._2, r._2), cgroup.plus(l._3, r._3), dgroup.plus(l._4, r._4), egroup.plus(l._5, r._5), fgroup.plus(l._6, r._6), ggroup.plus(l._7, r._7), hgroup.plus(l._8, r._8), igroup.plus(l._9, r._9), jgroup.plus(l._10, r._10), kgroup.plus(l._11, r._11), lgroup.plus(l._12, r._12), mgroup.plus(l._13, r._13), ngroup.plus(l._14, r._14), ogroup.plus(l._15, r._15), pgroup.plus(l._16, r._16), qgroup.plus(l._17, r._17), rgroup.plus(l._18, r._18))
  override def minus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)) = (agroup.minus(l._1, r._1), bgroup.minus(l._2, r._2), cgroup.minus(l._3, r._3), dgroup.minus(l._4, r._4), egroup.minus(l._5, r._5), fgroup.minus(l._6, r._6), ggroup.minus(l._7, r._7), hgroup.minus(l._8, r._8), igroup.minus(l._9, r._9), jgroup.minus(l._10, r._10), kgroup.minus(l._11, r._11), lgroup.minus(l._12, r._12), mgroup.minus(l._13, r._13), ngroup.minus(l._14, r._14), ogroup.minus(l._15, r._15), pgroup.minus(l._16, r._16), qgroup.minus(l._17, r._17), rgroup.minus(l._18, r._18))
}

/**
* Combine 18 rings into a product ring
*/
class Tuple18Ring[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I], jring : Ring[J], kring : Ring[K], lring : Ring[L], mring : Ring[M], nring : Ring[N], oring : Ring[O], pring : Ring[P], qring : Ring[Q], rring : Ring[R]) extends Ring[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)] {
  override def zero = (aring.zero, bring.zero, cring.zero, dring.zero, ering.zero, fring.zero, gring.zero, hring.zero, iring.zero, jring.zero, kring.zero, lring.zero, mring.zero, nring.zero, oring.zero, pring.zero, qring.zero, rring.zero)
  override def one = (aring.one, bring.one, cring.one, dring.one, ering.one, fring.one, gring.one, hring.one, iring.one, jring.one, kring.one, lring.one, mring.one, nring.one, oring.one, pring.one, qring.one, rring.one)
  override def negate(v : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)) = (aring.negate(v._1), bring.negate(v._2), cring.negate(v._3), dring.negate(v._4), ering.negate(v._5), fring.negate(v._6), gring.negate(v._7), hring.negate(v._8), iring.negate(v._9), jring.negate(v._10), kring.negate(v._11), lring.negate(v._12), mring.negate(v._13), nring.negate(v._14), oring.negate(v._15), pring.negate(v._16), qring.negate(v._17), rring.negate(v._18))
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)) = (aring.plus(l._1, r._1), bring.plus(l._2, r._2), cring.plus(l._3, r._3), dring.plus(l._4, r._4), ering.plus(l._5, r._5), fring.plus(l._6, r._6), gring.plus(l._7, r._7), hring.plus(l._8, r._8), iring.plus(l._9, r._9), jring.plus(l._10, r._10), kring.plus(l._11, r._11), lring.plus(l._12, r._12), mring.plus(l._13, r._13), nring.plus(l._14, r._14), oring.plus(l._15, r._15), pring.plus(l._16, r._16), qring.plus(l._17, r._17), rring.plus(l._18, r._18))
  override def minus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)) = (aring.minus(l._1, r._1), bring.minus(l._2, r._2), cring.minus(l._3, r._3), dring.minus(l._4, r._4), ering.minus(l._5, r._5), fring.minus(l._6, r._6), gring.minus(l._7, r._7), hring.minus(l._8, r._8), iring.minus(l._9, r._9), jring.minus(l._10, r._10), kring.minus(l._11, r._11), lring.minus(l._12, r._12), mring.minus(l._13, r._13), nring.minus(l._14, r._14), oring.minus(l._15, r._15), pring.minus(l._16, r._16), qring.minus(l._17, r._17), rring.minus(l._18, r._18))
  override def times(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)) = (aring.times(l._1, r._1), bring.times(l._2, r._2), cring.times(l._3, r._3), dring.times(l._4, r._4), ering.times(l._5, r._5), fring.times(l._6, r._6), gring.times(l._7, r._7), hring.times(l._8, r._8), iring.times(l._9, r._9), jring.times(l._10, r._10), kring.times(l._11, r._11), lring.times(l._12, r._12), mring.times(l._13, r._13), nring.times(l._14, r._14), oring.times(l._15, r._15), pring.times(l._16, r._16), qring.times(l._17, r._17), rring.times(l._18, r._18))
}
/**
* Combine 19 monoids into a product monoid
*/
class Tuple19Monoid[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I], jmonoid : Monoid[J], kmonoid : Monoid[K], lmonoid : Monoid[L], mmonoid : Monoid[M], nmonoid : Monoid[N], omonoid : Monoid[O], pmonoid : Monoid[P], qmonoid : Monoid[Q], rmonoid : Monoid[R], smonoid : Monoid[S]) extends Monoid[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)] {
  override def zero = (amonoid.zero, bmonoid.zero, cmonoid.zero, dmonoid.zero, emonoid.zero, fmonoid.zero, gmonoid.zero, hmonoid.zero, imonoid.zero, jmonoid.zero, kmonoid.zero, lmonoid.zero, mmonoid.zero, nmonoid.zero, omonoid.zero, pmonoid.zero, qmonoid.zero, rmonoid.zero, smonoid.zero)
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)) = (amonoid.plus(l._1, r._1), bmonoid.plus(l._2, r._2), cmonoid.plus(l._3, r._3), dmonoid.plus(l._4, r._4), emonoid.plus(l._5, r._5), fmonoid.plus(l._6, r._6), gmonoid.plus(l._7, r._7), hmonoid.plus(l._8, r._8), imonoid.plus(l._9, r._9), jmonoid.plus(l._10, r._10), kmonoid.plus(l._11, r._11), lmonoid.plus(l._12, r._12), mmonoid.plus(l._13, r._13), nmonoid.plus(l._14, r._14), omonoid.plus(l._15, r._15), pmonoid.plus(l._16, r._16), qmonoid.plus(l._17, r._17), rmonoid.plus(l._18, r._18), smonoid.plus(l._19, r._19))
}

/**
* Combine 19 groups into a product group
*/
class Tuple19Group[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I], jgroup : Group[J], kgroup : Group[K], lgroup : Group[L], mgroup : Group[M], ngroup : Group[N], ogroup : Group[O], pgroup : Group[P], qgroup : Group[Q], rgroup : Group[R], sgroup : Group[S]) extends Group[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)] {
  override def zero = (agroup.zero, bgroup.zero, cgroup.zero, dgroup.zero, egroup.zero, fgroup.zero, ggroup.zero, hgroup.zero, igroup.zero, jgroup.zero, kgroup.zero, lgroup.zero, mgroup.zero, ngroup.zero, ogroup.zero, pgroup.zero, qgroup.zero, rgroup.zero, sgroup.zero)
  override def negate(v : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)) = (agroup.negate(v._1), bgroup.negate(v._2), cgroup.negate(v._3), dgroup.negate(v._4), egroup.negate(v._5), fgroup.negate(v._6), ggroup.negate(v._7), hgroup.negate(v._8), igroup.negate(v._9), jgroup.negate(v._10), kgroup.negate(v._11), lgroup.negate(v._12), mgroup.negate(v._13), ngroup.negate(v._14), ogroup.negate(v._15), pgroup.negate(v._16), qgroup.negate(v._17), rgroup.negate(v._18), sgroup.negate(v._19))
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)) = (agroup.plus(l._1, r._1), bgroup.plus(l._2, r._2), cgroup.plus(l._3, r._3), dgroup.plus(l._4, r._4), egroup.plus(l._5, r._5), fgroup.plus(l._6, r._6), ggroup.plus(l._7, r._7), hgroup.plus(l._8, r._8), igroup.plus(l._9, r._9), jgroup.plus(l._10, r._10), kgroup.plus(l._11, r._11), lgroup.plus(l._12, r._12), mgroup.plus(l._13, r._13), ngroup.plus(l._14, r._14), ogroup.plus(l._15, r._15), pgroup.plus(l._16, r._16), qgroup.plus(l._17, r._17), rgroup.plus(l._18, r._18), sgroup.plus(l._19, r._19))
  override def minus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)) = (agroup.minus(l._1, r._1), bgroup.minus(l._2, r._2), cgroup.minus(l._3, r._3), dgroup.minus(l._4, r._4), egroup.minus(l._5, r._5), fgroup.minus(l._6, r._6), ggroup.minus(l._7, r._7), hgroup.minus(l._8, r._8), igroup.minus(l._9, r._9), jgroup.minus(l._10, r._10), kgroup.minus(l._11, r._11), lgroup.minus(l._12, r._12), mgroup.minus(l._13, r._13), ngroup.minus(l._14, r._14), ogroup.minus(l._15, r._15), pgroup.minus(l._16, r._16), qgroup.minus(l._17, r._17), rgroup.minus(l._18, r._18), sgroup.minus(l._19, r._19))
}

/**
* Combine 19 rings into a product ring
*/
class Tuple19Ring[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I], jring : Ring[J], kring : Ring[K], lring : Ring[L], mring : Ring[M], nring : Ring[N], oring : Ring[O], pring : Ring[P], qring : Ring[Q], rring : Ring[R], sring : Ring[S]) extends Ring[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)] {
  override def zero = (aring.zero, bring.zero, cring.zero, dring.zero, ering.zero, fring.zero, gring.zero, hring.zero, iring.zero, jring.zero, kring.zero, lring.zero, mring.zero, nring.zero, oring.zero, pring.zero, qring.zero, rring.zero, sring.zero)
  override def one = (aring.one, bring.one, cring.one, dring.one, ering.one, fring.one, gring.one, hring.one, iring.one, jring.one, kring.one, lring.one, mring.one, nring.one, oring.one, pring.one, qring.one, rring.one, sring.one)
  override def negate(v : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)) = (aring.negate(v._1), bring.negate(v._2), cring.negate(v._3), dring.negate(v._4), ering.negate(v._5), fring.negate(v._6), gring.negate(v._7), hring.negate(v._8), iring.negate(v._9), jring.negate(v._10), kring.negate(v._11), lring.negate(v._12), mring.negate(v._13), nring.negate(v._14), oring.negate(v._15), pring.negate(v._16), qring.negate(v._17), rring.negate(v._18), sring.negate(v._19))
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)) = (aring.plus(l._1, r._1), bring.plus(l._2, r._2), cring.plus(l._3, r._3), dring.plus(l._4, r._4), ering.plus(l._5, r._5), fring.plus(l._6, r._6), gring.plus(l._7, r._7), hring.plus(l._8, r._8), iring.plus(l._9, r._9), jring.plus(l._10, r._10), kring.plus(l._11, r._11), lring.plus(l._12, r._12), mring.plus(l._13, r._13), nring.plus(l._14, r._14), oring.plus(l._15, r._15), pring.plus(l._16, r._16), qring.plus(l._17, r._17), rring.plus(l._18, r._18), sring.plus(l._19, r._19))
  override def minus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)) = (aring.minus(l._1, r._1), bring.minus(l._2, r._2), cring.minus(l._3, r._3), dring.minus(l._4, r._4), ering.minus(l._5, r._5), fring.minus(l._6, r._6), gring.minus(l._7, r._7), hring.minus(l._8, r._8), iring.minus(l._9, r._9), jring.minus(l._10, r._10), kring.minus(l._11, r._11), lring.minus(l._12, r._12), mring.minus(l._13, r._13), nring.minus(l._14, r._14), oring.minus(l._15, r._15), pring.minus(l._16, r._16), qring.minus(l._17, r._17), rring.minus(l._18, r._18), sring.minus(l._19, r._19))
  override def times(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)) = (aring.times(l._1, r._1), bring.times(l._2, r._2), cring.times(l._3, r._3), dring.times(l._4, r._4), ering.times(l._5, r._5), fring.times(l._6, r._6), gring.times(l._7, r._7), hring.times(l._8, r._8), iring.times(l._9, r._9), jring.times(l._10, r._10), kring.times(l._11, r._11), lring.times(l._12, r._12), mring.times(l._13, r._13), nring.times(l._14, r._14), oring.times(l._15, r._15), pring.times(l._16, r._16), qring.times(l._17, r._17), rring.times(l._18, r._18), sring.times(l._19, r._19))
}
/**
* Combine 20 monoids into a product monoid
*/
class Tuple20Monoid[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I], jmonoid : Monoid[J], kmonoid : Monoid[K], lmonoid : Monoid[L], mmonoid : Monoid[M], nmonoid : Monoid[N], omonoid : Monoid[O], pmonoid : Monoid[P], qmonoid : Monoid[Q], rmonoid : Monoid[R], smonoid : Monoid[S], tmonoid : Monoid[T]) extends Monoid[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)] {
  override def zero = (amonoid.zero, bmonoid.zero, cmonoid.zero, dmonoid.zero, emonoid.zero, fmonoid.zero, gmonoid.zero, hmonoid.zero, imonoid.zero, jmonoid.zero, kmonoid.zero, lmonoid.zero, mmonoid.zero, nmonoid.zero, omonoid.zero, pmonoid.zero, qmonoid.zero, rmonoid.zero, smonoid.zero, tmonoid.zero)
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)) = (amonoid.plus(l._1, r._1), bmonoid.plus(l._2, r._2), cmonoid.plus(l._3, r._3), dmonoid.plus(l._4, r._4), emonoid.plus(l._5, r._5), fmonoid.plus(l._6, r._6), gmonoid.plus(l._7, r._7), hmonoid.plus(l._8, r._8), imonoid.plus(l._9, r._9), jmonoid.plus(l._10, r._10), kmonoid.plus(l._11, r._11), lmonoid.plus(l._12, r._12), mmonoid.plus(l._13, r._13), nmonoid.plus(l._14, r._14), omonoid.plus(l._15, r._15), pmonoid.plus(l._16, r._16), qmonoid.plus(l._17, r._17), rmonoid.plus(l._18, r._18), smonoid.plus(l._19, r._19), tmonoid.plus(l._20, r._20))
}

/**
* Combine 20 groups into a product group
*/
class Tuple20Group[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I], jgroup : Group[J], kgroup : Group[K], lgroup : Group[L], mgroup : Group[M], ngroup : Group[N], ogroup : Group[O], pgroup : Group[P], qgroup : Group[Q], rgroup : Group[R], sgroup : Group[S], tgroup : Group[T]) extends Group[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)] {
  override def zero = (agroup.zero, bgroup.zero, cgroup.zero, dgroup.zero, egroup.zero, fgroup.zero, ggroup.zero, hgroup.zero, igroup.zero, jgroup.zero, kgroup.zero, lgroup.zero, mgroup.zero, ngroup.zero, ogroup.zero, pgroup.zero, qgroup.zero, rgroup.zero, sgroup.zero, tgroup.zero)
  override def negate(v : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)) = (agroup.negate(v._1), bgroup.negate(v._2), cgroup.negate(v._3), dgroup.negate(v._4), egroup.negate(v._5), fgroup.negate(v._6), ggroup.negate(v._7), hgroup.negate(v._8), igroup.negate(v._9), jgroup.negate(v._10), kgroup.negate(v._11), lgroup.negate(v._12), mgroup.negate(v._13), ngroup.negate(v._14), ogroup.negate(v._15), pgroup.negate(v._16), qgroup.negate(v._17), rgroup.negate(v._18), sgroup.negate(v._19), tgroup.negate(v._20))
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)) = (agroup.plus(l._1, r._1), bgroup.plus(l._2, r._2), cgroup.plus(l._3, r._3), dgroup.plus(l._4, r._4), egroup.plus(l._5, r._5), fgroup.plus(l._6, r._6), ggroup.plus(l._7, r._7), hgroup.plus(l._8, r._8), igroup.plus(l._9, r._9), jgroup.plus(l._10, r._10), kgroup.plus(l._11, r._11), lgroup.plus(l._12, r._12), mgroup.plus(l._13, r._13), ngroup.plus(l._14, r._14), ogroup.plus(l._15, r._15), pgroup.plus(l._16, r._16), qgroup.plus(l._17, r._17), rgroup.plus(l._18, r._18), sgroup.plus(l._19, r._19), tgroup.plus(l._20, r._20))
  override def minus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)) = (agroup.minus(l._1, r._1), bgroup.minus(l._2, r._2), cgroup.minus(l._3, r._3), dgroup.minus(l._4, r._4), egroup.minus(l._5, r._5), fgroup.minus(l._6, r._6), ggroup.minus(l._7, r._7), hgroup.minus(l._8, r._8), igroup.minus(l._9, r._9), jgroup.minus(l._10, r._10), kgroup.minus(l._11, r._11), lgroup.minus(l._12, r._12), mgroup.minus(l._13, r._13), ngroup.minus(l._14, r._14), ogroup.minus(l._15, r._15), pgroup.minus(l._16, r._16), qgroup.minus(l._17, r._17), rgroup.minus(l._18, r._18), sgroup.minus(l._19, r._19), tgroup.minus(l._20, r._20))
}

/**
* Combine 20 rings into a product ring
*/
class Tuple20Ring[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I], jring : Ring[J], kring : Ring[K], lring : Ring[L], mring : Ring[M], nring : Ring[N], oring : Ring[O], pring : Ring[P], qring : Ring[Q], rring : Ring[R], sring : Ring[S], tring : Ring[T]) extends Ring[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)] {
  override def zero = (aring.zero, bring.zero, cring.zero, dring.zero, ering.zero, fring.zero, gring.zero, hring.zero, iring.zero, jring.zero, kring.zero, lring.zero, mring.zero, nring.zero, oring.zero, pring.zero, qring.zero, rring.zero, sring.zero, tring.zero)
  override def one = (aring.one, bring.one, cring.one, dring.one, ering.one, fring.one, gring.one, hring.one, iring.one, jring.one, kring.one, lring.one, mring.one, nring.one, oring.one, pring.one, qring.one, rring.one, sring.one, tring.one)
  override def negate(v : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)) = (aring.negate(v._1), bring.negate(v._2), cring.negate(v._3), dring.negate(v._4), ering.negate(v._5), fring.negate(v._6), gring.negate(v._7), hring.negate(v._8), iring.negate(v._9), jring.negate(v._10), kring.negate(v._11), lring.negate(v._12), mring.negate(v._13), nring.negate(v._14), oring.negate(v._15), pring.negate(v._16), qring.negate(v._17), rring.negate(v._18), sring.negate(v._19), tring.negate(v._20))
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)) = (aring.plus(l._1, r._1), bring.plus(l._2, r._2), cring.plus(l._3, r._3), dring.plus(l._4, r._4), ering.plus(l._5, r._5), fring.plus(l._6, r._6), gring.plus(l._7, r._7), hring.plus(l._8, r._8), iring.plus(l._9, r._9), jring.plus(l._10, r._10), kring.plus(l._11, r._11), lring.plus(l._12, r._12), mring.plus(l._13, r._13), nring.plus(l._14, r._14), oring.plus(l._15, r._15), pring.plus(l._16, r._16), qring.plus(l._17, r._17), rring.plus(l._18, r._18), sring.plus(l._19, r._19), tring.plus(l._20, r._20))
  override def minus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)) = (aring.minus(l._1, r._1), bring.minus(l._2, r._2), cring.minus(l._3, r._3), dring.minus(l._4, r._4), ering.minus(l._5, r._5), fring.minus(l._6, r._6), gring.minus(l._7, r._7), hring.minus(l._8, r._8), iring.minus(l._9, r._9), jring.minus(l._10, r._10), kring.minus(l._11, r._11), lring.minus(l._12, r._12), mring.minus(l._13, r._13), nring.minus(l._14, r._14), oring.minus(l._15, r._15), pring.minus(l._16, r._16), qring.minus(l._17, r._17), rring.minus(l._18, r._18), sring.minus(l._19, r._19), tring.minus(l._20, r._20))
  override def times(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)) = (aring.times(l._1, r._1), bring.times(l._2, r._2), cring.times(l._3, r._3), dring.times(l._4, r._4), ering.times(l._5, r._5), fring.times(l._6, r._6), gring.times(l._7, r._7), hring.times(l._8, r._8), iring.times(l._9, r._9), jring.times(l._10, r._10), kring.times(l._11, r._11), lring.times(l._12, r._12), mring.times(l._13, r._13), nring.times(l._14, r._14), oring.times(l._15, r._15), pring.times(l._16, r._16), qring.times(l._17, r._17), rring.times(l._18, r._18), sring.times(l._19, r._19), tring.times(l._20, r._20))
}
/**
* Combine 21 monoids into a product monoid
*/
class Tuple21Monoid[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I], jmonoid : Monoid[J], kmonoid : Monoid[K], lmonoid : Monoid[L], mmonoid : Monoid[M], nmonoid : Monoid[N], omonoid : Monoid[O], pmonoid : Monoid[P], qmonoid : Monoid[Q], rmonoid : Monoid[R], smonoid : Monoid[S], tmonoid : Monoid[T], umonoid : Monoid[U]) extends Monoid[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)] {
  override def zero = (amonoid.zero, bmonoid.zero, cmonoid.zero, dmonoid.zero, emonoid.zero, fmonoid.zero, gmonoid.zero, hmonoid.zero, imonoid.zero, jmonoid.zero, kmonoid.zero, lmonoid.zero, mmonoid.zero, nmonoid.zero, omonoid.zero, pmonoid.zero, qmonoid.zero, rmonoid.zero, smonoid.zero, tmonoid.zero, umonoid.zero)
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)) = (amonoid.plus(l._1, r._1), bmonoid.plus(l._2, r._2), cmonoid.plus(l._3, r._3), dmonoid.plus(l._4, r._4), emonoid.plus(l._5, r._5), fmonoid.plus(l._6, r._6), gmonoid.plus(l._7, r._7), hmonoid.plus(l._8, r._8), imonoid.plus(l._9, r._9), jmonoid.plus(l._10, r._10), kmonoid.plus(l._11, r._11), lmonoid.plus(l._12, r._12), mmonoid.plus(l._13, r._13), nmonoid.plus(l._14, r._14), omonoid.plus(l._15, r._15), pmonoid.plus(l._16, r._16), qmonoid.plus(l._17, r._17), rmonoid.plus(l._18, r._18), smonoid.plus(l._19, r._19), tmonoid.plus(l._20, r._20), umonoid.plus(l._21, r._21))
}

/**
* Combine 21 groups into a product group
*/
class Tuple21Group[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I], jgroup : Group[J], kgroup : Group[K], lgroup : Group[L], mgroup : Group[M], ngroup : Group[N], ogroup : Group[O], pgroup : Group[P], qgroup : Group[Q], rgroup : Group[R], sgroup : Group[S], tgroup : Group[T], ugroup : Group[U]) extends Group[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)] {
  override def zero = (agroup.zero, bgroup.zero, cgroup.zero, dgroup.zero, egroup.zero, fgroup.zero, ggroup.zero, hgroup.zero, igroup.zero, jgroup.zero, kgroup.zero, lgroup.zero, mgroup.zero, ngroup.zero, ogroup.zero, pgroup.zero, qgroup.zero, rgroup.zero, sgroup.zero, tgroup.zero, ugroup.zero)
  override def negate(v : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)) = (agroup.negate(v._1), bgroup.negate(v._2), cgroup.negate(v._3), dgroup.negate(v._4), egroup.negate(v._5), fgroup.negate(v._6), ggroup.negate(v._7), hgroup.negate(v._8), igroup.negate(v._9), jgroup.negate(v._10), kgroup.negate(v._11), lgroup.negate(v._12), mgroup.negate(v._13), ngroup.negate(v._14), ogroup.negate(v._15), pgroup.negate(v._16), qgroup.negate(v._17), rgroup.negate(v._18), sgroup.negate(v._19), tgroup.negate(v._20), ugroup.negate(v._21))
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)) = (agroup.plus(l._1, r._1), bgroup.plus(l._2, r._2), cgroup.plus(l._3, r._3), dgroup.plus(l._4, r._4), egroup.plus(l._5, r._5), fgroup.plus(l._6, r._6), ggroup.plus(l._7, r._7), hgroup.plus(l._8, r._8), igroup.plus(l._9, r._9), jgroup.plus(l._10, r._10), kgroup.plus(l._11, r._11), lgroup.plus(l._12, r._12), mgroup.plus(l._13, r._13), ngroup.plus(l._14, r._14), ogroup.plus(l._15, r._15), pgroup.plus(l._16, r._16), qgroup.plus(l._17, r._17), rgroup.plus(l._18, r._18), sgroup.plus(l._19, r._19), tgroup.plus(l._20, r._20), ugroup.plus(l._21, r._21))
  override def minus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)) = (agroup.minus(l._1, r._1), bgroup.minus(l._2, r._2), cgroup.minus(l._3, r._3), dgroup.minus(l._4, r._4), egroup.minus(l._5, r._5), fgroup.minus(l._6, r._6), ggroup.minus(l._7, r._7), hgroup.minus(l._8, r._8), igroup.minus(l._9, r._9), jgroup.minus(l._10, r._10), kgroup.minus(l._11, r._11), lgroup.minus(l._12, r._12), mgroup.minus(l._13, r._13), ngroup.minus(l._14, r._14), ogroup.minus(l._15, r._15), pgroup.minus(l._16, r._16), qgroup.minus(l._17, r._17), rgroup.minus(l._18, r._18), sgroup.minus(l._19, r._19), tgroup.minus(l._20, r._20), ugroup.minus(l._21, r._21))
}

/**
* Combine 21 rings into a product ring
*/
class Tuple21Ring[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I], jring : Ring[J], kring : Ring[K], lring : Ring[L], mring : Ring[M], nring : Ring[N], oring : Ring[O], pring : Ring[P], qring : Ring[Q], rring : Ring[R], sring : Ring[S], tring : Ring[T], uring : Ring[U]) extends Ring[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)] {
  override def zero = (aring.zero, bring.zero, cring.zero, dring.zero, ering.zero, fring.zero, gring.zero, hring.zero, iring.zero, jring.zero, kring.zero, lring.zero, mring.zero, nring.zero, oring.zero, pring.zero, qring.zero, rring.zero, sring.zero, tring.zero, uring.zero)
  override def one = (aring.one, bring.one, cring.one, dring.one, ering.one, fring.one, gring.one, hring.one, iring.one, jring.one, kring.one, lring.one, mring.one, nring.one, oring.one, pring.one, qring.one, rring.one, sring.one, tring.one, uring.one)
  override def negate(v : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)) = (aring.negate(v._1), bring.negate(v._2), cring.negate(v._3), dring.negate(v._4), ering.negate(v._5), fring.negate(v._6), gring.negate(v._7), hring.negate(v._8), iring.negate(v._9), jring.negate(v._10), kring.negate(v._11), lring.negate(v._12), mring.negate(v._13), nring.negate(v._14), oring.negate(v._15), pring.negate(v._16), qring.negate(v._17), rring.negate(v._18), sring.negate(v._19), tring.negate(v._20), uring.negate(v._21))
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)) = (aring.plus(l._1, r._1), bring.plus(l._2, r._2), cring.plus(l._3, r._3), dring.plus(l._4, r._4), ering.plus(l._5, r._5), fring.plus(l._6, r._6), gring.plus(l._7, r._7), hring.plus(l._8, r._8), iring.plus(l._9, r._9), jring.plus(l._10, r._10), kring.plus(l._11, r._11), lring.plus(l._12, r._12), mring.plus(l._13, r._13), nring.plus(l._14, r._14), oring.plus(l._15, r._15), pring.plus(l._16, r._16), qring.plus(l._17, r._17), rring.plus(l._18, r._18), sring.plus(l._19, r._19), tring.plus(l._20, r._20), uring.plus(l._21, r._21))
  override def minus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)) = (aring.minus(l._1, r._1), bring.minus(l._2, r._2), cring.minus(l._3, r._3), dring.minus(l._4, r._4), ering.minus(l._5, r._5), fring.minus(l._6, r._6), gring.minus(l._7, r._7), hring.minus(l._8, r._8), iring.minus(l._9, r._9), jring.minus(l._10, r._10), kring.minus(l._11, r._11), lring.minus(l._12, r._12), mring.minus(l._13, r._13), nring.minus(l._14, r._14), oring.minus(l._15, r._15), pring.minus(l._16, r._16), qring.minus(l._17, r._17), rring.minus(l._18, r._18), sring.minus(l._19, r._19), tring.minus(l._20, r._20), uring.minus(l._21, r._21))
  override def times(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)) = (aring.times(l._1, r._1), bring.times(l._2, r._2), cring.times(l._3, r._3), dring.times(l._4, r._4), ering.times(l._5, r._5), fring.times(l._6, r._6), gring.times(l._7, r._7), hring.times(l._8, r._8), iring.times(l._9, r._9), jring.times(l._10, r._10), kring.times(l._11, r._11), lring.times(l._12, r._12), mring.times(l._13, r._13), nring.times(l._14, r._14), oring.times(l._15, r._15), pring.times(l._16, r._16), qring.times(l._17, r._17), rring.times(l._18, r._18), sring.times(l._19, r._19), tring.times(l._20, r._20), uring.times(l._21, r._21))
}
/**
* Combine 22 monoids into a product monoid
*/
class Tuple22Monoid[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I], jmonoid : Monoid[J], kmonoid : Monoid[K], lmonoid : Monoid[L], mmonoid : Monoid[M], nmonoid : Monoid[N], omonoid : Monoid[O], pmonoid : Monoid[P], qmonoid : Monoid[Q], rmonoid : Monoid[R], smonoid : Monoid[S], tmonoid : Monoid[T], umonoid : Monoid[U], vmonoid : Monoid[V]) extends Monoid[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)] {
  override def zero = (amonoid.zero, bmonoid.zero, cmonoid.zero, dmonoid.zero, emonoid.zero, fmonoid.zero, gmonoid.zero, hmonoid.zero, imonoid.zero, jmonoid.zero, kmonoid.zero, lmonoid.zero, mmonoid.zero, nmonoid.zero, omonoid.zero, pmonoid.zero, qmonoid.zero, rmonoid.zero, smonoid.zero, tmonoid.zero, umonoid.zero, vmonoid.zero)
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)) = (amonoid.plus(l._1, r._1), bmonoid.plus(l._2, r._2), cmonoid.plus(l._3, r._3), dmonoid.plus(l._4, r._4), emonoid.plus(l._5, r._5), fmonoid.plus(l._6, r._6), gmonoid.plus(l._7, r._7), hmonoid.plus(l._8, r._8), imonoid.plus(l._9, r._9), jmonoid.plus(l._10, r._10), kmonoid.plus(l._11, r._11), lmonoid.plus(l._12, r._12), mmonoid.plus(l._13, r._13), nmonoid.plus(l._14, r._14), omonoid.plus(l._15, r._15), pmonoid.plus(l._16, r._16), qmonoid.plus(l._17, r._17), rmonoid.plus(l._18, r._18), smonoid.plus(l._19, r._19), tmonoid.plus(l._20, r._20), umonoid.plus(l._21, r._21), vmonoid.plus(l._22, r._22))
}

/**
* Combine 22 groups into a product group
*/
class Tuple22Group[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I], jgroup : Group[J], kgroup : Group[K], lgroup : Group[L], mgroup : Group[M], ngroup : Group[N], ogroup : Group[O], pgroup : Group[P], qgroup : Group[Q], rgroup : Group[R], sgroup : Group[S], tgroup : Group[T], ugroup : Group[U], vgroup : Group[V]) extends Group[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)] {
  override def zero = (agroup.zero, bgroup.zero, cgroup.zero, dgroup.zero, egroup.zero, fgroup.zero, ggroup.zero, hgroup.zero, igroup.zero, jgroup.zero, kgroup.zero, lgroup.zero, mgroup.zero, ngroup.zero, ogroup.zero, pgroup.zero, qgroup.zero, rgroup.zero, sgroup.zero, tgroup.zero, ugroup.zero, vgroup.zero)
  override def negate(v : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)) = (agroup.negate(v._1), bgroup.negate(v._2), cgroup.negate(v._3), dgroup.negate(v._4), egroup.negate(v._5), fgroup.negate(v._6), ggroup.negate(v._7), hgroup.negate(v._8), igroup.negate(v._9), jgroup.negate(v._10), kgroup.negate(v._11), lgroup.negate(v._12), mgroup.negate(v._13), ngroup.negate(v._14), ogroup.negate(v._15), pgroup.negate(v._16), qgroup.negate(v._17), rgroup.negate(v._18), sgroup.negate(v._19), tgroup.negate(v._20), ugroup.negate(v._21), vgroup.negate(v._22))
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)) = (agroup.plus(l._1, r._1), bgroup.plus(l._2, r._2), cgroup.plus(l._3, r._3), dgroup.plus(l._4, r._4), egroup.plus(l._5, r._5), fgroup.plus(l._6, r._6), ggroup.plus(l._7, r._7), hgroup.plus(l._8, r._8), igroup.plus(l._9, r._9), jgroup.plus(l._10, r._10), kgroup.plus(l._11, r._11), lgroup.plus(l._12, r._12), mgroup.plus(l._13, r._13), ngroup.plus(l._14, r._14), ogroup.plus(l._15, r._15), pgroup.plus(l._16, r._16), qgroup.plus(l._17, r._17), rgroup.plus(l._18, r._18), sgroup.plus(l._19, r._19), tgroup.plus(l._20, r._20), ugroup.plus(l._21, r._21), vgroup.plus(l._22, r._22))
  override def minus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)) = (agroup.minus(l._1, r._1), bgroup.minus(l._2, r._2), cgroup.minus(l._3, r._3), dgroup.minus(l._4, r._4), egroup.minus(l._5, r._5), fgroup.minus(l._6, r._6), ggroup.minus(l._7, r._7), hgroup.minus(l._8, r._8), igroup.minus(l._9, r._9), jgroup.minus(l._10, r._10), kgroup.minus(l._11, r._11), lgroup.minus(l._12, r._12), mgroup.minus(l._13, r._13), ngroup.minus(l._14, r._14), ogroup.minus(l._15, r._15), pgroup.minus(l._16, r._16), qgroup.minus(l._17, r._17), rgroup.minus(l._18, r._18), sgroup.minus(l._19, r._19), tgroup.minus(l._20, r._20), ugroup.minus(l._21, r._21), vgroup.minus(l._22, r._22))
}

/**
* Combine 22 rings into a product ring
*/
class Tuple22Ring[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I], jring : Ring[J], kring : Ring[K], lring : Ring[L], mring : Ring[M], nring : Ring[N], oring : Ring[O], pring : Ring[P], qring : Ring[Q], rring : Ring[R], sring : Ring[S], tring : Ring[T], uring : Ring[U], vring : Ring[V]) extends Ring[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)] {
  override def zero = (aring.zero, bring.zero, cring.zero, dring.zero, ering.zero, fring.zero, gring.zero, hring.zero, iring.zero, jring.zero, kring.zero, lring.zero, mring.zero, nring.zero, oring.zero, pring.zero, qring.zero, rring.zero, sring.zero, tring.zero, uring.zero, vring.zero)
  override def one = (aring.one, bring.one, cring.one, dring.one, ering.one, fring.one, gring.one, hring.one, iring.one, jring.one, kring.one, lring.one, mring.one, nring.one, oring.one, pring.one, qring.one, rring.one, sring.one, tring.one, uring.one, vring.one)
  override def negate(v : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)) = (aring.negate(v._1), bring.negate(v._2), cring.negate(v._3), dring.negate(v._4), ering.negate(v._5), fring.negate(v._6), gring.negate(v._7), hring.negate(v._8), iring.negate(v._9), jring.negate(v._10), kring.negate(v._11), lring.negate(v._12), mring.negate(v._13), nring.negate(v._14), oring.negate(v._15), pring.negate(v._16), qring.negate(v._17), rring.negate(v._18), sring.negate(v._19), tring.negate(v._20), uring.negate(v._21), vring.negate(v._22))
  override def plus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)) = (aring.plus(l._1, r._1), bring.plus(l._2, r._2), cring.plus(l._3, r._3), dring.plus(l._4, r._4), ering.plus(l._5, r._5), fring.plus(l._6, r._6), gring.plus(l._7, r._7), hring.plus(l._8, r._8), iring.plus(l._9, r._9), jring.plus(l._10, r._10), kring.plus(l._11, r._11), lring.plus(l._12, r._12), mring.plus(l._13, r._13), nring.plus(l._14, r._14), oring.plus(l._15, r._15), pring.plus(l._16, r._16), qring.plus(l._17, r._17), rring.plus(l._18, r._18), sring.plus(l._19, r._19), tring.plus(l._20, r._20), uring.plus(l._21, r._21), vring.plus(l._22, r._22))
  override def minus(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)) = (aring.minus(l._1, r._1), bring.minus(l._2, r._2), cring.minus(l._3, r._3), dring.minus(l._4, r._4), ering.minus(l._5, r._5), fring.minus(l._6, r._6), gring.minus(l._7, r._7), hring.minus(l._8, r._8), iring.minus(l._9, r._9), jring.minus(l._10, r._10), kring.minus(l._11, r._11), lring.minus(l._12, r._12), mring.minus(l._13, r._13), nring.minus(l._14, r._14), oring.minus(l._15, r._15), pring.minus(l._16, r._16), qring.minus(l._17, r._17), rring.minus(l._18, r._18), sring.minus(l._19, r._19), tring.minus(l._20, r._20), uring.minus(l._21, r._21), vring.minus(l._22, r._22))
  override def times(l : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V), r : (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)) = (aring.times(l._1, r._1), bring.times(l._2, r._2), cring.times(l._3, r._3), dring.times(l._4, r._4), ering.times(l._5, r._5), fring.times(l._6, r._6), gring.times(l._7, r._7), hring.times(l._8, r._8), iring.times(l._9, r._9), jring.times(l._10, r._10), kring.times(l._11, r._11), lring.times(l._12, r._12), mring.times(l._13, r._13), nring.times(l._14, r._14), oring.times(l._15, r._15), pring.times(l._16, r._16), qring.times(l._17, r._17), rring.times(l._18, r._18), sring.times(l._19, r._19), tring.times(l._20, r._20), uring.times(l._21, r._21), vring.times(l._22, r._22))
}

// Here we complete the typeclasses by added the implicits to the companions:

object Monoid {
  implicit val boolMonoid : Monoid[Boolean] = BooleanField
  implicit val intMonoid : Monoid[Int] = IntRing
  implicit val longMonoid : Monoid[Long] = LongRing
  implicit val floatMonoid : Monoid[Float] = FloatField
  implicit val doubleMonoid : Monoid[Double] = DoubleField
  implicit def listMonoid[T] : Monoid[List[T]] = new ListMonoid[T]
  implicit def setMonoid[T] : Monoid[Set[T]] = new SetMonoid[T]
  implicit def mapMonoid[K,V](implicit monoid : Monoid[V]) = new MapMonoid[K,V]()(monoid)

  implicit def pairMonoid[T,U](implicit tg : Monoid[T], ug : Monoid[U]) : Monoid[(T,U)] = {
    new Tuple2Monoid[T,U]()(tg,ug)
  }
  implicit def monoid3[A, B, C](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C]) : Monoid[(A, B, C)] = {
    new Tuple3Monoid[A, B, C]()(amonoid, bmonoid, cmonoid)
  }

  implicit def monoid4[A, B, C, D](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D]) : Monoid[(A, B, C, D)] = {
    new Tuple4Monoid[A, B, C, D]()(amonoid, bmonoid, cmonoid, dmonoid)
  }

  implicit def monoid5[A, B, C, D, E](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E]) : Monoid[(A, B, C, D, E)] = {
    new Tuple5Monoid[A, B, C, D, E]()(amonoid, bmonoid, cmonoid, dmonoid, emonoid)
  }

  implicit def monoid6[A, B, C, D, E, F](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F]) : Monoid[(A, B, C, D, E, F)] = {
    new Tuple6Monoid[A, B, C, D, E, F]()(amonoid, bmonoid, cmonoid, dmonoid, emonoid, fmonoid)
  }

  implicit def monoid7[A, B, C, D, E, F, G](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G]) : Monoid[(A, B, C, D, E, F, G)] = {
    new Tuple7Monoid[A, B, C, D, E, F, G]()(amonoid, bmonoid, cmonoid, dmonoid, emonoid, fmonoid, gmonoid)
  }

  implicit def monoid8[A, B, C, D, E, F, G, H](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H]) : Monoid[(A, B, C, D, E, F, G, H)] = {
    new Tuple8Monoid[A, B, C, D, E, F, G, H]()(amonoid, bmonoid, cmonoid, dmonoid, emonoid, fmonoid, gmonoid, hmonoid)
  }

  implicit def monoid9[A, B, C, D, E, F, G, H, I](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I]) : Monoid[(A, B, C, D, E, F, G, H, I)] = {
    new Tuple9Monoid[A, B, C, D, E, F, G, H, I]()(amonoid, bmonoid, cmonoid, dmonoid, emonoid, fmonoid, gmonoid, hmonoid, imonoid)
  }

  implicit def monoid10[A, B, C, D, E, F, G, H, I, J](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I], jmonoid : Monoid[J]) : Monoid[(A, B, C, D, E, F, G, H, I, J)] = {
    new Tuple10Monoid[A, B, C, D, E, F, G, H, I, J]()(amonoid, bmonoid, cmonoid, dmonoid, emonoid, fmonoid, gmonoid, hmonoid, imonoid, jmonoid)
  }

  implicit def monoid11[A, B, C, D, E, F, G, H, I, J, K](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I], jmonoid : Monoid[J], kmonoid : Monoid[K]) : Monoid[(A, B, C, D, E, F, G, H, I, J, K)] = {
    new Tuple11Monoid[A, B, C, D, E, F, G, H, I, J, K]()(amonoid, bmonoid, cmonoid, dmonoid, emonoid, fmonoid, gmonoid, hmonoid, imonoid, jmonoid, kmonoid)
  }

  implicit def monoid12[A, B, C, D, E, F, G, H, I, J, K, L](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I], jmonoid : Monoid[J], kmonoid : Monoid[K], lmonoid : Monoid[L]) : Monoid[(A, B, C, D, E, F, G, H, I, J, K, L)] = {
    new Tuple12Monoid[A, B, C, D, E, F, G, H, I, J, K, L]()(amonoid, bmonoid, cmonoid, dmonoid, emonoid, fmonoid, gmonoid, hmonoid, imonoid, jmonoid, kmonoid, lmonoid)
  }

  implicit def monoid13[A, B, C, D, E, F, G, H, I, J, K, L, M](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I], jmonoid : Monoid[J], kmonoid : Monoid[K], lmonoid : Monoid[L], mmonoid : Monoid[M]) : Monoid[(A, B, C, D, E, F, G, H, I, J, K, L, M)] = {
    new Tuple13Monoid[A, B, C, D, E, F, G, H, I, J, K, L, M]()(amonoid, bmonoid, cmonoid, dmonoid, emonoid, fmonoid, gmonoid, hmonoid, imonoid, jmonoid, kmonoid, lmonoid, mmonoid)
  }

  implicit def monoid14[A, B, C, D, E, F, G, H, I, J, K, L, M, N](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I], jmonoid : Monoid[J], kmonoid : Monoid[K], lmonoid : Monoid[L], mmonoid : Monoid[M], nmonoid : Monoid[N]) : Monoid[(A, B, C, D, E, F, G, H, I, J, K, L, M, N)] = {
    new Tuple14Monoid[A, B, C, D, E, F, G, H, I, J, K, L, M, N]()(amonoid, bmonoid, cmonoid, dmonoid, emonoid, fmonoid, gmonoid, hmonoid, imonoid, jmonoid, kmonoid, lmonoid, mmonoid, nmonoid)
  }

  implicit def monoid15[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I], jmonoid : Monoid[J], kmonoid : Monoid[K], lmonoid : Monoid[L], mmonoid : Monoid[M], nmonoid : Monoid[N], omonoid : Monoid[O]) : Monoid[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)] = {
    new Tuple15Monoid[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O]()(amonoid, bmonoid, cmonoid, dmonoid, emonoid, fmonoid, gmonoid, hmonoid, imonoid, jmonoid, kmonoid, lmonoid, mmonoid, nmonoid, omonoid)
  }

  implicit def monoid16[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I], jmonoid : Monoid[J], kmonoid : Monoid[K], lmonoid : Monoid[L], mmonoid : Monoid[M], nmonoid : Monoid[N], omonoid : Monoid[O], pmonoid : Monoid[P]) : Monoid[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)] = {
    new Tuple16Monoid[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P]()(amonoid, bmonoid, cmonoid, dmonoid, emonoid, fmonoid, gmonoid, hmonoid, imonoid, jmonoid, kmonoid, lmonoid, mmonoid, nmonoid, omonoid, pmonoid)
  }

  implicit def monoid17[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I], jmonoid : Monoid[J], kmonoid : Monoid[K], lmonoid : Monoid[L], mmonoid : Monoid[M], nmonoid : Monoid[N], omonoid : Monoid[O], pmonoid : Monoid[P], qmonoid : Monoid[Q]) : Monoid[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)] = {
    new Tuple17Monoid[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q]()(amonoid, bmonoid, cmonoid, dmonoid, emonoid, fmonoid, gmonoid, hmonoid, imonoid, jmonoid, kmonoid, lmonoid, mmonoid, nmonoid, omonoid, pmonoid, qmonoid)
  }

  implicit def monoid18[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I], jmonoid : Monoid[J], kmonoid : Monoid[K], lmonoid : Monoid[L], mmonoid : Monoid[M], nmonoid : Monoid[N], omonoid : Monoid[O], pmonoid : Monoid[P], qmonoid : Monoid[Q], rmonoid : Monoid[R]) : Monoid[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)] = {
    new Tuple18Monoid[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R]()(amonoid, bmonoid, cmonoid, dmonoid, emonoid, fmonoid, gmonoid, hmonoid, imonoid, jmonoid, kmonoid, lmonoid, mmonoid, nmonoid, omonoid, pmonoid, qmonoid, rmonoid)
  }

  implicit def monoid19[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I], jmonoid : Monoid[J], kmonoid : Monoid[K], lmonoid : Monoid[L], mmonoid : Monoid[M], nmonoid : Monoid[N], omonoid : Monoid[O], pmonoid : Monoid[P], qmonoid : Monoid[Q], rmonoid : Monoid[R], smonoid : Monoid[S]) : Monoid[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)] = {
    new Tuple19Monoid[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S]()(amonoid, bmonoid, cmonoid, dmonoid, emonoid, fmonoid, gmonoid, hmonoid, imonoid, jmonoid, kmonoid, lmonoid, mmonoid, nmonoid, omonoid, pmonoid, qmonoid, rmonoid, smonoid)
  }

  implicit def monoid20[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I], jmonoid : Monoid[J], kmonoid : Monoid[K], lmonoid : Monoid[L], mmonoid : Monoid[M], nmonoid : Monoid[N], omonoid : Monoid[O], pmonoid : Monoid[P], qmonoid : Monoid[Q], rmonoid : Monoid[R], smonoid : Monoid[S], tmonoid : Monoid[T]) : Monoid[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)] = {
    new Tuple20Monoid[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T]()(amonoid, bmonoid, cmonoid, dmonoid, emonoid, fmonoid, gmonoid, hmonoid, imonoid, jmonoid, kmonoid, lmonoid, mmonoid, nmonoid, omonoid, pmonoid, qmonoid, rmonoid, smonoid, tmonoid)
  }

  implicit def monoid21[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I], jmonoid : Monoid[J], kmonoid : Monoid[K], lmonoid : Monoid[L], mmonoid : Monoid[M], nmonoid : Monoid[N], omonoid : Monoid[O], pmonoid : Monoid[P], qmonoid : Monoid[Q], rmonoid : Monoid[R], smonoid : Monoid[S], tmonoid : Monoid[T], umonoid : Monoid[U]) : Monoid[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)] = {
    new Tuple21Monoid[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U]()(amonoid, bmonoid, cmonoid, dmonoid, emonoid, fmonoid, gmonoid, hmonoid, imonoid, jmonoid, kmonoid, lmonoid, mmonoid, nmonoid, omonoid, pmonoid, qmonoid, rmonoid, smonoid, tmonoid, umonoid)
  }

  implicit def monoid22[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V](implicit amonoid : Monoid[A], bmonoid : Monoid[B], cmonoid : Monoid[C], dmonoid : Monoid[D], emonoid : Monoid[E], fmonoid : Monoid[F], gmonoid : Monoid[G], hmonoid : Monoid[H], imonoid : Monoid[I], jmonoid : Monoid[J], kmonoid : Monoid[K], lmonoid : Monoid[L], mmonoid : Monoid[M], nmonoid : Monoid[N], omonoid : Monoid[O], pmonoid : Monoid[P], qmonoid : Monoid[Q], rmonoid : Monoid[R], smonoid : Monoid[S], tmonoid : Monoid[T], umonoid : Monoid[U], vmonoid : Monoid[V]) : Monoid[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)] = {
    new Tuple22Monoid[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V]()(amonoid, bmonoid, cmonoid, dmonoid, emonoid, fmonoid, gmonoid, hmonoid, imonoid, jmonoid, kmonoid, lmonoid, mmonoid, nmonoid, omonoid, pmonoid, qmonoid, rmonoid, smonoid, tmonoid, umonoid, vmonoid)
  }
}
object Group {
  implicit val boolGroup : Group[Boolean] = BooleanField
  implicit val intGroup : Group[Int] = IntRing
  implicit val longGroup : Group[Long] = LongRing
  implicit val floatGroup : Group[Float] = FloatField
  implicit val doubleGroup : Group[Double] = DoubleField
  implicit def mapGroup[K,V](implicit group : Group[V]) = new MapGroup[K,V]()(group)
  implicit def pairGroup[T,U](implicit tg : Group[T], ug : Group[U]) : Group[(T,U)] = {
    new Tuple2Group[T,U]()(tg,ug)
  }
  implicit def group3[A, B, C](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C]) : Group[(A, B, C)] = {
    new Tuple3Group[A, B, C]()(agroup, bgroup, cgroup)
  }

  implicit def group4[A, B, C, D](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D]) : Group[(A, B, C, D)] = {
    new Tuple4Group[A, B, C, D]()(agroup, bgroup, cgroup, dgroup)
  }

  implicit def group5[A, B, C, D, E](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E]) : Group[(A, B, C, D, E)] = {
    new Tuple5Group[A, B, C, D, E]()(agroup, bgroup, cgroup, dgroup, egroup)
  }

  implicit def group6[A, B, C, D, E, F](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F]) : Group[(A, B, C, D, E, F)] = {
    new Tuple6Group[A, B, C, D, E, F]()(agroup, bgroup, cgroup, dgroup, egroup, fgroup)
  }

  implicit def group7[A, B, C, D, E, F, G](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G]) : Group[(A, B, C, D, E, F, G)] = {
    new Tuple7Group[A, B, C, D, E, F, G]()(agroup, bgroup, cgroup, dgroup, egroup, fgroup, ggroup)
  }

  implicit def group8[A, B, C, D, E, F, G, H](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H]) : Group[(A, B, C, D, E, F, G, H)] = {
    new Tuple8Group[A, B, C, D, E, F, G, H]()(agroup, bgroup, cgroup, dgroup, egroup, fgroup, ggroup, hgroup)
  }

  implicit def group9[A, B, C, D, E, F, G, H, I](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I]) : Group[(A, B, C, D, E, F, G, H, I)] = {
    new Tuple9Group[A, B, C, D, E, F, G, H, I]()(agroup, bgroup, cgroup, dgroup, egroup, fgroup, ggroup, hgroup, igroup)
  }

  implicit def group10[A, B, C, D, E, F, G, H, I, J](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I], jgroup : Group[J]) : Group[(A, B, C, D, E, F, G, H, I, J)] = {
    new Tuple10Group[A, B, C, D, E, F, G, H, I, J]()(agroup, bgroup, cgroup, dgroup, egroup, fgroup, ggroup, hgroup, igroup, jgroup)
  }

  implicit def group11[A, B, C, D, E, F, G, H, I, J, K](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I], jgroup : Group[J], kgroup : Group[K]) : Group[(A, B, C, D, E, F, G, H, I, J, K)] = {
    new Tuple11Group[A, B, C, D, E, F, G, H, I, J, K]()(agroup, bgroup, cgroup, dgroup, egroup, fgroup, ggroup, hgroup, igroup, jgroup, kgroup)
  }

  implicit def group12[A, B, C, D, E, F, G, H, I, J, K, L](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I], jgroup : Group[J], kgroup : Group[K], lgroup : Group[L]) : Group[(A, B, C, D, E, F, G, H, I, J, K, L)] = {
    new Tuple12Group[A, B, C, D, E, F, G, H, I, J, K, L]()(agroup, bgroup, cgroup, dgroup, egroup, fgroup, ggroup, hgroup, igroup, jgroup, kgroup, lgroup)
  }

  implicit def group13[A, B, C, D, E, F, G, H, I, J, K, L, M](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I], jgroup : Group[J], kgroup : Group[K], lgroup : Group[L], mgroup : Group[M]) : Group[(A, B, C, D, E, F, G, H, I, J, K, L, M)] = {
    new Tuple13Group[A, B, C, D, E, F, G, H, I, J, K, L, M]()(agroup, bgroup, cgroup, dgroup, egroup, fgroup, ggroup, hgroup, igroup, jgroup, kgroup, lgroup, mgroup)
  }

  implicit def group14[A, B, C, D, E, F, G, H, I, J, K, L, M, N](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I], jgroup : Group[J], kgroup : Group[K], lgroup : Group[L], mgroup : Group[M], ngroup : Group[N]) : Group[(A, B, C, D, E, F, G, H, I, J, K, L, M, N)] = {
    new Tuple14Group[A, B, C, D, E, F, G, H, I, J, K, L, M, N]()(agroup, bgroup, cgroup, dgroup, egroup, fgroup, ggroup, hgroup, igroup, jgroup, kgroup, lgroup, mgroup, ngroup)
  }

  implicit def group15[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I], jgroup : Group[J], kgroup : Group[K], lgroup : Group[L], mgroup : Group[M], ngroup : Group[N], ogroup : Group[O]) : Group[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)] = {
    new Tuple15Group[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O]()(agroup, bgroup, cgroup, dgroup, egroup, fgroup, ggroup, hgroup, igroup, jgroup, kgroup, lgroup, mgroup, ngroup, ogroup)
  }

  implicit def group16[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I], jgroup : Group[J], kgroup : Group[K], lgroup : Group[L], mgroup : Group[M], ngroup : Group[N], ogroup : Group[O], pgroup : Group[P]) : Group[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)] = {
    new Tuple16Group[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P]()(agroup, bgroup, cgroup, dgroup, egroup, fgroup, ggroup, hgroup, igroup, jgroup, kgroup, lgroup, mgroup, ngroup, ogroup, pgroup)
  }

  implicit def group17[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I], jgroup : Group[J], kgroup : Group[K], lgroup : Group[L], mgroup : Group[M], ngroup : Group[N], ogroup : Group[O], pgroup : Group[P], qgroup : Group[Q]) : Group[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)] = {
    new Tuple17Group[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q]()(agroup, bgroup, cgroup, dgroup, egroup, fgroup, ggroup, hgroup, igroup, jgroup, kgroup, lgroup, mgroup, ngroup, ogroup, pgroup, qgroup)
  }

  implicit def group18[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I], jgroup : Group[J], kgroup : Group[K], lgroup : Group[L], mgroup : Group[M], ngroup : Group[N], ogroup : Group[O], pgroup : Group[P], qgroup : Group[Q], rgroup : Group[R]) : Group[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)] = {
    new Tuple18Group[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R]()(agroup, bgroup, cgroup, dgroup, egroup, fgroup, ggroup, hgroup, igroup, jgroup, kgroup, lgroup, mgroup, ngroup, ogroup, pgroup, qgroup, rgroup)
  }

  implicit def group19[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I], jgroup : Group[J], kgroup : Group[K], lgroup : Group[L], mgroup : Group[M], ngroup : Group[N], ogroup : Group[O], pgroup : Group[P], qgroup : Group[Q], rgroup : Group[R], sgroup : Group[S]) : Group[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)] = {
    new Tuple19Group[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S]()(agroup, bgroup, cgroup, dgroup, egroup, fgroup, ggroup, hgroup, igroup, jgroup, kgroup, lgroup, mgroup, ngroup, ogroup, pgroup, qgroup, rgroup, sgroup)
  }

  implicit def group20[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I], jgroup : Group[J], kgroup : Group[K], lgroup : Group[L], mgroup : Group[M], ngroup : Group[N], ogroup : Group[O], pgroup : Group[P], qgroup : Group[Q], rgroup : Group[R], sgroup : Group[S], tgroup : Group[T]) : Group[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)] = {
    new Tuple20Group[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T]()(agroup, bgroup, cgroup, dgroup, egroup, fgroup, ggroup, hgroup, igroup, jgroup, kgroup, lgroup, mgroup, ngroup, ogroup, pgroup, qgroup, rgroup, sgroup, tgroup)
  }

  implicit def group21[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I], jgroup : Group[J], kgroup : Group[K], lgroup : Group[L], mgroup : Group[M], ngroup : Group[N], ogroup : Group[O], pgroup : Group[P], qgroup : Group[Q], rgroup : Group[R], sgroup : Group[S], tgroup : Group[T], ugroup : Group[U]) : Group[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)] = {
    new Tuple21Group[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U]()(agroup, bgroup, cgroup, dgroup, egroup, fgroup, ggroup, hgroup, igroup, jgroup, kgroup, lgroup, mgroup, ngroup, ogroup, pgroup, qgroup, rgroup, sgroup, tgroup, ugroup)
  }

  implicit def group22[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V](implicit agroup : Group[A], bgroup : Group[B], cgroup : Group[C], dgroup : Group[D], egroup : Group[E], fgroup : Group[F], ggroup : Group[G], hgroup : Group[H], igroup : Group[I], jgroup : Group[J], kgroup : Group[K], lgroup : Group[L], mgroup : Group[M], ngroup : Group[N], ogroup : Group[O], pgroup : Group[P], qgroup : Group[Q], rgroup : Group[R], sgroup : Group[S], tgroup : Group[T], ugroup : Group[U], vgroup : Group[V]) : Group[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)] = {
    new Tuple22Group[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V]()(agroup, bgroup, cgroup, dgroup, egroup, fgroup, ggroup, hgroup, igroup, jgroup, kgroup, lgroup, mgroup, ngroup, ogroup, pgroup, qgroup, rgroup, sgroup, tgroup, ugroup, vgroup)
  }
}
object Ring {
  implicit val boolRing : Ring[Boolean] = BooleanField
  implicit val intRing : Ring[Int] = IntRing
  implicit val longRing : Ring[Long] = LongRing
  implicit val floatRing : Ring[Float] = FloatField
  implicit val doubleRing : Ring[Double] = DoubleField
  implicit def mapRing[K,V](implicit ring : Ring[V]) = new MapRing[K,V]()(ring)
  implicit def pairRing[T,U](implicit tr : Ring[T], ur : Ring[U]) : Ring[(T,U)] = {
    new Tuple2Ring[T,U]()(tr,ur)
  }
  implicit def ring3[A, B, C](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C]) : Ring[(A, B, C)] = {
    new Tuple3Ring[A, B, C]()(aring, bring, cring)
  }

  implicit def ring4[A, B, C, D](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D]) : Ring[(A, B, C, D)] = {
    new Tuple4Ring[A, B, C, D]()(aring, bring, cring, dring)
  }

  implicit def ring5[A, B, C, D, E](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E]) : Ring[(A, B, C, D, E)] = {
    new Tuple5Ring[A, B, C, D, E]()(aring, bring, cring, dring, ering)
  }

  implicit def ring6[A, B, C, D, E, F](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F]) : Ring[(A, B, C, D, E, F)] = {
    new Tuple6Ring[A, B, C, D, E, F]()(aring, bring, cring, dring, ering, fring)
  }

  implicit def ring7[A, B, C, D, E, F, G](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G]) : Ring[(A, B, C, D, E, F, G)] = {
    new Tuple7Ring[A, B, C, D, E, F, G]()(aring, bring, cring, dring, ering, fring, gring)
  }

  implicit def ring8[A, B, C, D, E, F, G, H](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H]) : Ring[(A, B, C, D, E, F, G, H)] = {
    new Tuple8Ring[A, B, C, D, E, F, G, H]()(aring, bring, cring, dring, ering, fring, gring, hring)
  }

  implicit def ring9[A, B, C, D, E, F, G, H, I](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I]) : Ring[(A, B, C, D, E, F, G, H, I)] = {
    new Tuple9Ring[A, B, C, D, E, F, G, H, I]()(aring, bring, cring, dring, ering, fring, gring, hring, iring)
  }

  implicit def ring10[A, B, C, D, E, F, G, H, I, J](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I], jring : Ring[J]) : Ring[(A, B, C, D, E, F, G, H, I, J)] = {
    new Tuple10Ring[A, B, C, D, E, F, G, H, I, J]()(aring, bring, cring, dring, ering, fring, gring, hring, iring, jring)
  }

  implicit def ring11[A, B, C, D, E, F, G, H, I, J, K](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I], jring : Ring[J], kring : Ring[K]) : Ring[(A, B, C, D, E, F, G, H, I, J, K)] = {
    new Tuple11Ring[A, B, C, D, E, F, G, H, I, J, K]()(aring, bring, cring, dring, ering, fring, gring, hring, iring, jring, kring)
  }

  implicit def ring12[A, B, C, D, E, F, G, H, I, J, K, L](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I], jring : Ring[J], kring : Ring[K], lring : Ring[L]) : Ring[(A, B, C, D, E, F, G, H, I, J, K, L)] = {
    new Tuple12Ring[A, B, C, D, E, F, G, H, I, J, K, L]()(aring, bring, cring, dring, ering, fring, gring, hring, iring, jring, kring, lring)
  }

  implicit def ring13[A, B, C, D, E, F, G, H, I, J, K, L, M](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I], jring : Ring[J], kring : Ring[K], lring : Ring[L], mring : Ring[M]) : Ring[(A, B, C, D, E, F, G, H, I, J, K, L, M)] = {
    new Tuple13Ring[A, B, C, D, E, F, G, H, I, J, K, L, M]()(aring, bring, cring, dring, ering, fring, gring, hring, iring, jring, kring, lring, mring)
  }

  implicit def ring14[A, B, C, D, E, F, G, H, I, J, K, L, M, N](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I], jring : Ring[J], kring : Ring[K], lring : Ring[L], mring : Ring[M], nring : Ring[N]) : Ring[(A, B, C, D, E, F, G, H, I, J, K, L, M, N)] = {
    new Tuple14Ring[A, B, C, D, E, F, G, H, I, J, K, L, M, N]()(aring, bring, cring, dring, ering, fring, gring, hring, iring, jring, kring, lring, mring, nring)
  }

  implicit def ring15[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I], jring : Ring[J], kring : Ring[K], lring : Ring[L], mring : Ring[M], nring : Ring[N], oring : Ring[O]) : Ring[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)] = {
    new Tuple15Ring[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O]()(aring, bring, cring, dring, ering, fring, gring, hring, iring, jring, kring, lring, mring, nring, oring)
  }

  implicit def ring16[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I], jring : Ring[J], kring : Ring[K], lring : Ring[L], mring : Ring[M], nring : Ring[N], oring : Ring[O], pring : Ring[P]) : Ring[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)] = {
    new Tuple16Ring[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P]()(aring, bring, cring, dring, ering, fring, gring, hring, iring, jring, kring, lring, mring, nring, oring, pring)
  }

  implicit def ring17[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I], jring : Ring[J], kring : Ring[K], lring : Ring[L], mring : Ring[M], nring : Ring[N], oring : Ring[O], pring : Ring[P], qring : Ring[Q]) : Ring[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)] = {
    new Tuple17Ring[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q]()(aring, bring, cring, dring, ering, fring, gring, hring, iring, jring, kring, lring, mring, nring, oring, pring, qring)
  }

  implicit def ring18[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I], jring : Ring[J], kring : Ring[K], lring : Ring[L], mring : Ring[M], nring : Ring[N], oring : Ring[O], pring : Ring[P], qring : Ring[Q], rring : Ring[R]) : Ring[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)] = {
    new Tuple18Ring[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R]()(aring, bring, cring, dring, ering, fring, gring, hring, iring, jring, kring, lring, mring, nring, oring, pring, qring, rring)
  }

  implicit def ring19[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I], jring : Ring[J], kring : Ring[K], lring : Ring[L], mring : Ring[M], nring : Ring[N], oring : Ring[O], pring : Ring[P], qring : Ring[Q], rring : Ring[R], sring : Ring[S]) : Ring[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)] = {
    new Tuple19Ring[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S]()(aring, bring, cring, dring, ering, fring, gring, hring, iring, jring, kring, lring, mring, nring, oring, pring, qring, rring, sring)
  }

  implicit def ring20[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I], jring : Ring[J], kring : Ring[K], lring : Ring[L], mring : Ring[M], nring : Ring[N], oring : Ring[O], pring : Ring[P], qring : Ring[Q], rring : Ring[R], sring : Ring[S], tring : Ring[T]) : Ring[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)] = {
    new Tuple20Ring[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T]()(aring, bring, cring, dring, ering, fring, gring, hring, iring, jring, kring, lring, mring, nring, oring, pring, qring, rring, sring, tring)
  }

  implicit def ring21[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I], jring : Ring[J], kring : Ring[K], lring : Ring[L], mring : Ring[M], nring : Ring[N], oring : Ring[O], pring : Ring[P], qring : Ring[Q], rring : Ring[R], sring : Ring[S], tring : Ring[T], uring : Ring[U]) : Ring[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)] = {
    new Tuple21Ring[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U]()(aring, bring, cring, dring, ering, fring, gring, hring, iring, jring, kring, lring, mring, nring, oring, pring, qring, rring, sring, tring, uring)
  }

  implicit def ring22[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V](implicit aring : Ring[A], bring : Ring[B], cring : Ring[C], dring : Ring[D], ering : Ring[E], fring : Ring[F], gring : Ring[G], hring : Ring[H], iring : Ring[I], jring : Ring[J], kring : Ring[K], lring : Ring[L], mring : Ring[M], nring : Ring[N], oring : Ring[O], pring : Ring[P], qring : Ring[Q], rring : Ring[R], sring : Ring[S], tring : Ring[T], uring : Ring[U], vring : Ring[V]) : Ring[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)] = {
    new Tuple22Ring[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V]()(aring, bring, cring, dring, ering, fring, gring, hring, iring, jring, kring, lring, mring, nring, oring, pring, qring, rring, sring, tring, uring, vring)
  }

}

object Field {
  implicit val boolField : Field[Boolean] = BooleanField
  implicit val floatField : Field[Float] = FloatField
  implicit val doubleField : Field[Double] = DoubleField
}
