package com.twitter.scalding.typed.gen

sealed abstract class TypeWith[+Ev[_]] {
  type Type
  def evidence: Ev[Type]
}

object TypeWith {
  type Aux[A, Ev[_]] = TypeWith[Ev] { type Type = A }

  def apply[A, Ev[_]](implicit eva: Ev[A]): Aux[A, Ev] =
    new TypeWith[Ev] {
      type Type = A
      def evidence: Ev[Type] = eva
    }
}
