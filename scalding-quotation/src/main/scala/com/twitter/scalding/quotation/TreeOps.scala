package com.twitter.scalding.quotation

import scala.reflect.macros.blackbox.Context

trait TreeOps {
  val c: Context
  import c.universe._

  /**
   * Finds the first tree that satisfies the condition.
   */
  def find(tree: Tree)(f: Tree => Boolean): Option[Tree] = {
    var res: Option[Tree] = None
    val t = new Traverser {
      override def traverse(t: Tree) = {
        if (res.isEmpty)
          if (f(t))
            res = Some(t)
          else
            super.traverse(t)
      }
    }
    t.traverse(tree)
    res
  }

  /**
   * Similar to tree.collect but it doesn't collect the children of a
   * collected tree.
   */
  def collect[T](tree: Tree)(f: PartialFunction[Tree, T]): List[T] = {
    var res = List.newBuilder[T]
    val t = new Traverser {
      override def traverse(t: Tree) = {
        f.lift(t) match {
          case Some(v) =>
            res += v
          case None =>
            super.traverse(t)
        }
      }
    }
    t.traverse(tree)
    res.result()
  }
}