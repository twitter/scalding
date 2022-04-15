package com.twitter.scalding.dagon

import scala.util.control.TailCalls.{done, tailcall, TailRec}

object Memoize {

  /**
   * Allow the user to create memoized recursive functions, by providing a function which can operate values
   * as well as references to "itself".
   *
   * For example, we can translate the naive recursive Fibonnaci definition (which is exponential) into an
   * opimized linear-time (and linear-space) form:
   *
   * Memoize.function[Int, Long] { case (0, _) => 1 case (1, _) => 1 case (i, f) => f(i - 1) + f(i - 2) }
   */
  def function[A, B](f: (A, A => B) => B): A => B = {
    // It is tempting to use a mutable.Map here,
    // but mutating the Map inside of the call-by-name value causes
    // some issues in some versions of scala. It is
    // safer to use a mutable pointer to an immutable Map.
    val cache = Cache.empty[A, B]
    lazy val g: A => B = (a: A) => cache.getOrElseUpdate(a, f(a, g))
    g
  }

  type RecursiveK[A[_], B[_]] = FunctionK[Lambda[x => (A[x], FunctionK[A, B])], B]

  /**
   * Memoize a FunctionK using an HCache internally.
   */
  def functionK[A[_], B[_]](f: RecursiveK[A, B]): FunctionK[A, B] = {
    val hcache = HCache.empty[A, B]
    lazy val hg: FunctionK[A, B] = new FunctionK[A, B] {
      def toFunction[T]: A[T] => B[T] =
        at => hcache.getOrElseUpdate(at, f((at, hg)))
    }
    hg
  }

  private def cacheCall[A](t: => TailRec[A]): TailRec[A] = {
    var res: Option[TailRec[A]] = None
    tailcall {
      res match {
        case Some(a) => a
        case None =>
          t.flatMap { a =>
            val d = done(a)
            res = Some(d)
            d
          }
      }
    }
  }

  type FunctionKRec[A[_], B[_]] = FunctionK[A, Lambda[x => TailRec[B[x]]]]
  type RecursiveKTailRec[A[_], B[_]] =
    FunctionK[Lambda[x => (A[x], FunctionKRec[A, B])], Lambda[x => TailRec[B[x]]]]

  /**
   * Memoize a FunctionK using an HCache, and tailCalls, which are slower but make things stack safe
   */
  def functionKTailRec[A[_], B[_]](f: RecursiveKTailRec[A, B]): FunctionKRec[A, B] = {
    type TailB[Z] = TailRec[B[Z]]
    val hcache = HCache.empty[A, TailB]
    lazy val hg: FunctionK[A, TailB] = new FunctionK[A, TailB] {
      def toFunction[T]: A[T] => TailB[T] =
        at => hcache.getOrElseUpdate(at, cacheCall(f((at, hg))))
    }
    hg
  }
}
