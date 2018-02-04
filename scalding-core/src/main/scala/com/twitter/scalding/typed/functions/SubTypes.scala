package com.twitter.scalding.typed.functions

/**
 * This is a more powerful version of <:< that can allow
 * us to remove casts and also not have any runtime cost
 * for our function calls in some cases of trivial functions
 */
sealed abstract class SubTypes[-A, +B] extends java.io.Serializable {
  def apply(a: A): B
  def subst[F[-_]](f: F[B]): F[A]

  def toEv: A <:< B = {
    val aa = implicitly[B <:< B]
    type F[-T] = T <:< B
    subst[F](aa)
  }

  def liftCo[F[+_]]: SubTypes[F[A], F[B]] = {
    type G[-T] = SubTypes[F[T], F[B]]
    subst[G](SubTypes.fromSubType[F[B], F[B]])
  }
  /** create a new evidence for a contravariant type F[_]
   */
  def liftContra[F[-_]]: SubTypes[F[B], F[A]] = {
    type G[-T] = SubTypes[F[B], F[T]]
    subst[G](SubTypes.fromSubType[F[B], F[B]])
  }
}

object SubTypes extends java.io.Serializable {
  private[this] final case class ReflexiveSubTypes[A]() extends SubTypes[A, A] {
    def apply(a: A): A = a
    def subst[F[-_]](f: F[A]): F[A] = f
  }

  implicit def fromSubType[A, B >: A]: SubTypes[A, B] = ReflexiveSubTypes[A]()

  def fromEv[A, B](ev: A <:< B): SubTypes[A, B] = // linter:disable:UnusedParameter
    // in scala 2.13, this won't need a cast, but the cast is safe
    fromSubType[A, A].asInstanceOf[SubTypes[A, B]]
}

