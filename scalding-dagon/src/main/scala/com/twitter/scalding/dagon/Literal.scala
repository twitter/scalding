package com.twitter.scalding.dagon

import java.io.Serializable
import scala.util.control.TailCalls
import scala.util.hashing.MurmurHash3

/**
 * This represents literal expressions (no variable redirection)
 * of container nodes of type N[T]
 */
sealed trait Literal[N[_], T] extends Serializable { self: Product =>
  def evaluate: N[T] = Literal.evaluate(this)

  override val hashCode: Int = MurmurHash3.productHash(self)

  /**
   * Here we memoize as we check equality and always check reference
   * equality first. This can dramatically improve performance on
   * graphs that merge back often
   */
  override def equals(that: Any) = that match {
    case thatF: Literal[_, _] =>
      if (thatF eq this) true
      else if (thatF.hashCode != hashCode) false
      else Literal.eqFn[N](RefPair(this, thatF.asInstanceOf[Literal[N, _]]))
    case _ => false
  }
}

object Literal {

  sealed case class Const[N[_], T](override val evaluate: N[T]) extends Literal[N, T]

  sealed case class Unary[N[_], T1, T2](arg: Literal[N, T1], fn: N[T1] => N[T2]) extends Literal[N, T2]

  sealed case class Binary[N[_], T1, T2, T3](arg1: Literal[N, T1],
                                      arg2: Literal[N, T2],
                                      fn: (N[T1], N[T2]) => N[T3]) extends Literal[N, T3]

  sealed case class Variadic[N[_], T1, T2](args: List[Literal[N, T1]], fn: List[N[T1]] => N[T2]) extends Literal[N, T2]

  /**
   * This evaluates a literal formula back to what it represents
   * being careful to handle diamonds by creating referentially
   * equivalent structures (not just structurally equivalent)
   */
  def evaluate[N[_], T](lit: Literal[N, T]): N[T] =
    evaluateMemo[N](lit)

  /**
   * Memoized version of evaluation to handle diamonds
   *
   * Each call to this creates a new internal memo.
   */
  def evaluateMemo[N[_]]: FunctionK[Literal[N, *], N] = {
    import TailCalls._

    val slowAndSafe = Memoize.functionKTailRec[Literal[N, *], N](new Memoize.RecursiveKTailRec[Literal[N, *], N] {
      def toFunction[T] = {
        case (Const(n), _) => done(n)
        case (Unary(n, fn), rec) => rec(n).map(fn)
        case (Binary(n1, n2, fn), rec) =>
          for {
            nn1 <- rec(n1)
            nn2 <- rec(n2)
          } yield fn(nn1, nn2)
        case (Variadic(args, fn), rec) =>
          def loop[A](as: List[Literal[N, A]]): TailRec[List[N[A]]] =
            as match {
              case Nil => done(Nil)
              case h :: t => loop(t).flatMap(tt => rec(h).map(_ :: tt))
            }
          loop(args).map(fn)
      }
    })

    val fast = Memoize.functionK[Literal[N, *], N](new Memoize.RecursiveK[Literal[N, *], N] {
      def toFunction[T] = {
        case (Const(n), _) => n
        case (Unary(n, fn), rec) => fn(rec(n))
        case (Binary(n1, n2, fn), rec) => fn(rec(n1), rec(n2))
        case (Variadic(args, fn), rec) => fn(args.map(rec(_)))
      }
    })

    def onStackGoSlow[A](lit: Literal[N, A]): N[A] =
      try fast(lit)
      catch {
        case _: Throwable => //StackOverflowError should work, but not on scala.js
          slowAndSafe(lit).result
      }

    /*
     * We *non-recursively* use either the fast approach or the slow approach
     */
    Memoize.functionK[Literal[N, *], N](new Memoize.RecursiveK[Literal[N, *], N] {
      def toFunction[T] = { case (u, _) => onStackGoSlow(u) }
    })
  }

  /**
   * Note that this is a def, not a val, so the cache only lives
   * as long as a single outermost equality check
   */
  private def eqFn[N[_]]: Function[RefPair[Literal[N, _], Literal[N, _]], Boolean] =
    Memoize.function[RefPair[Literal[N, _], Literal[N, _]], Boolean] {
      case (pair, _) if pair.itemsEq => true
      case (RefPair(Const(a), Const(b)), _) => a == b
      case (RefPair(Unary(left, fa), Unary(right, fb)), rec) =>
        (fa == fb) && rec(RefPair(left, right))
      case (RefPair(Binary(lefta, righta, fa), Binary(leftb, rightb, fb)), rec) =>
        (fa == fb) && rec(RefPair(lefta, leftb)) && rec(RefPair(righta, rightb))
      case (RefPair(Variadic(argsa, fa), Variadic(argsb, fb)), rec) =>
        @annotation.tailrec
        def loop(left: List[Literal[N, _]], right: List[Literal[N, _]]): Boolean =
          (left, right) match {
            case (lh :: ltail, rh :: rtail) =>
              rec(RefPair(lh, rh)) && loop(ltail, rtail)
            case (Nil, Nil) => true
            case _ => false
          }

        (fa == fb) && loop(argsa, argsb)
      case other => false
    }
}
