package com.twitter.scalding.typed.gen


import com.twitter.algebird.Semigroup
import com.twitter.scalding.TupleConverter
import org.scalacheck.{Cogen, Gen}

trait TypeGen[A] {
  val gen: Gen[A]
  val cogen: Cogen[A]
  val ordering: Ordering[A]
  val semigroup: Semigroup[A]
  val tupleConverter: TupleConverter[A]
}

object TypeGen {
  import StdGen._
  import StdSemigroup._

  def apply[A](g: Gen[A], c: Cogen[A], o: Ordering[A], s: Semigroup[A], t: TupleConverter[A]): TypeGen[A] =
    new TypeGen[A] {
      val gen: Gen[A] = g
      val cogen: Cogen[A] = c
      val ordering: Ordering[A] = o
      val semigroup: Semigroup[A] = s
      val tupleConverter: TupleConverter[A] = t
    }

  def apply[A, B](a: TypeGen[A], b: TypeGen[B]): TypeGen[(A, B)] =
    new TypeGen[(A, B)] {
      val gen: Gen[(A, B)] = Gen.zip(a.gen, b.gen)
      val cogen: Cogen[(A, B)] = Cogen.tuple2(a.cogen, b.cogen)
      val ordering: Ordering[(A, B)] = Ordering.Tuple2(a.ordering, b.ordering)
      val semigroup: Semigroup[(A, B)] = Semigroup.semigroup2(a.semigroup, b.semigroup)
      val tupleConverter: TupleConverter[(A, B)] =
        TupleConverter.build(a.tupleConverter.arity + b.tupleConverter.arity) { te =>
          val ta = a.tupleConverter.apply(te)
          val tb = b.tupleConverter.apply(te)
          (ta, tb)
        }
    }

  implicit def typeGen[A: Gen: Cogen: Ordering: Semigroup: TupleConverter]: TypeGen[A] =
    TypeGen(implicitly, implicitly, implicitly, implicitly, implicitly)

  implicit val std: Gen[TypeWith[TypeGen]] =
    Gen.oneOf(
      TypeWith[Unit, TypeGen],
      TypeWith[Boolean, TypeGen],
      TypeWith[Byte, TypeGen],
      TypeWith[Char, TypeGen],
      TypeWith[Short, TypeGen],
      TypeWith[Int, TypeGen],
      TypeWith[Long, TypeGen],
      TypeWith[Float, TypeGen],
      TypeWith[Double, TypeGen],
      TypeWith[String, TypeGen]
    )
}
