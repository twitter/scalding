package com.twitter.scalding.typed.functions

/**
 * This is a more powerful version of =:= that can allow
 * us to remove casts and also not have any runtime cost
 * for our function calls in some cases of trivial functions
 */
sealed abstract class EqTypes[A, B] extends java.io.Serializable {
  def apply(a: A): B
  def subst[F[_]](f: F[A]): F[B]

  final def reverse: EqTypes[B, A] = {
    val aa = EqTypes.reflexive[A]
    type F[T] = EqTypes[T, A]
    subst[F](aa)
  }

  def toEv: A =:= B = {
    val aa = implicitly[A =:= A]
    type F[T] = A =:= T
    subst[F](aa)
  }
}

object EqTypes extends java.io.Serializable {
  private[this] final case class ReflexiveEquality[A]() extends EqTypes[A, A] {
    def apply(a: A): A = a
    def subst[F[_]](f: F[A]): F[A] = f
  }

  implicit def reflexive[A]: EqTypes[A, A] = ReflexiveEquality()

  def fromEv[A, B](ev: A =:= B): EqTypes[A, B] = // linter:disable:UnusedParameter
    // in scala 2.13, this won't need a cast, but the cast is safe
    reflexive[A].asInstanceOf[EqTypes[A, B]]
}

