package com.twitter.scalding.dagon

import java.io.Serializable

/**
 * This implements a simplification rule on Dags
 */
trait Rule[N[_]] extends Serializable { self =>

  /**
   * If the given Id can be replaced with a simpler expression, return Some(expr) else None.
   *
   * If it is convenient, you might write a partial function and then call .lift to get the correct Function
   * type
   */
  def apply[T](on: Dag[N]): N[T] => Option[N[T]]

  /**
   * If the current rule cannot apply, then try the argument here. Note, this applies in series at a given
   * node, not on the whole Dag after the first rule has run. For that, see Dag.applySeq.
   */
  def orElse(that: Rule[N]): Rule[N] =
    new Rule[N] {
      def apply[T](on: Dag[N]) = { n =>
        self.apply(on)(n) match {
          case Some(n1) if n1 == n =>
            // If the rule emits the same as input fall through
            that.apply(on)(n)
          case None =>
            that.apply(on)(n)
          case s @ Some(_) => s
        }
      }

      override def toString: String =
        s"$self.orElse($that)"
    }
}

object Rule {

  /**
   * A Rule that never applies
   */
  def empty[N[_]]: Rule[N] =
    new Rule[N] {
      def apply[T](on: Dag[N]) = { _ => None }
    }

  /**
   * Build a new Rule out of several using orElse to compose
   */
  def orElse[N[_]](it: Iterable[Rule[N]]): Rule[N] =
    it.reduceOption(_ orElse _).getOrElse(empty)
}
