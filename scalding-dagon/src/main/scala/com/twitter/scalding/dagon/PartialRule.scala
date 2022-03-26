package com.twitter.scalding.dagon

/**
 * Often a partial function is an easier way to express rules
 */
trait PartialRule[N[_]] extends Rule[N] {
  final def apply[T](on: Dag[N]): N[T] => Option[N[T]] =
    applyWhere[T](on).lift

  def applyWhere[T](on: Dag[N]): PartialFunction[N[T], N[T]]
}
