package com.twitter.scalding.dagon

import java.io.Serializable
/**
 * This is a Natural transformation.
 *
 * For any type X, this type can produce a function from T[X] to R[X].
 */
trait FunctionK[T[_], R[_]] extends Serializable {
  def apply[U](tu: T[U]): R[U] =
    toFunction[U](tu)

  def toFunction[U]: T[U] => R[U]
}

object FunctionK {
  def andThen[A[_], B[_], C[_]](first: FunctionK[A, B], second: FunctionK[B, C]): FunctionK[A, C] =
    new FunctionK[A, C] {
      def toFunction[U] = first.toFunction[U].andThen(second.toFunction[U])
    }
}
