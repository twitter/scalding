package com.twitter.scalding

import com.stripe.dagon.{Dag, FunctionK, Literal, Memoize, PartialRule, Rule}
import scala.annotation.tailrec
import scala.concurrent.{Future, ExecutionContext => ConcurrentExecutionContext}

object ExecutionOptimizationRules {
  type LiteralExecution[T] = Literal[Execution, T]

  /**
   * Since our Execution is covariant, but the Literal is not
   * this is actually safe in this context, but not in general
   */
  def widen[T](l: LiteralExecution[_ <: T]): LiteralExecution[T] = {
    // to prove this is safe, see that if you have
    // LiteralExecution[_ <: T] we can call .evaluate to get
    // Execution[_ <: T] which due to covariance is
    // Execution[T], and then using toLiteral we can get
    // LiteralExecution[T]
    //
    // that would be wasteful to apply since the final
    // result is identity.
    l.asInstanceOf[LiteralExecution[T]]
  }

  def toLiteral: FunctionK[Execution, LiteralExecution] =
    Memoize.functionK[Execution, LiteralExecution](
      new Memoize.RecursiveK[Execution, LiteralExecution] {
        override def toFunction[A] = {
          case (e@Execution.ReaderExecution, _) =>
            Literal.Const(e)
          case (e: Execution.FutureConst[a], _) =>
            Literal.Const(e)
          case (e: Execution.UniqueIdExecution[a], _) =>
            Literal.Const(e)
          case (e: Execution.FlowDefExecution, _) =>
            Literal.Const(e)
          case (e: Execution.WriteExecution[a], _) =>
            Literal.Const(e)
          case (e: Execution.GetCounters[a], f) =>
            widen(Literal.Unary[Execution, a, (a, ExecutionCounters)](f(e.prev), Execution.GetCounters(_: Execution[a])))
          case (e: Execution.ResetCounters[a], f) =>
            Literal.Unary(f(e.prev), Execution.ResetCounters(_: Execution[a]))
          case (e: Execution.WithNewCache[a], f) =>
            Literal.Unary(f(e.prev), Execution.WithNewCache(_: Execution[a]))
          case (e: Execution.TransformedConfig[a], f) =>
            Literal.Unary(f(e.prev), Execution.TransformedConfig(_: Execution[a], e.fn))
          case (e: Execution.OnComplete[a], f) =>
            Literal.Unary(f(e.prev), Execution.OnComplete(_: Execution[a], e.fn))
          case (e: Execution.RecoverWith[a], f) =>
            Literal.Unary(f(e.prev), Execution.RecoverWith(_: Execution[a], e.fn))
          case (e: Execution.Mapped[a, b], f) =>
            Literal.Unary(f(e.prev), Execution.Mapped(_: Execution[a], e.fn))
          case (e: Execution.FlatMapped[a, b], f) =>
            Literal.Unary(f(e.prev), Execution.FlatMapped(_: Execution[a], e.fn))
          case (e: Execution.Zipped[a, b], f) =>
            Literal.Binary(f(e.one), f(e.two), Execution.Zipped(_: Execution[a], _: Execution[b]))
        }
      }
    )

  /**
   * If `Execution` is `FlowDefExecution` or `WriteExecution`,
   * we are considering those executions as slow, since they will schedule some expensive work,
   * like Hadoop or Spark Job.
   *
   * If `Execution` is `FlatMapped` or `UniqueIdExecution`,
   * we are considering those executions as slow,
   * since we don't know which execution they can produce.
   *
   * Everything else we are considering as fast execution compare to `FlowDefExecution` and `WriteExecution`.
   */
  def isFastExecution[A](e: Execution[A]): Boolean =
    areFastExecution(List(e))

  /**
   * If `Execution` is `FlowDefExecution` or `WriteExecution`,
   * we are considering those executions as slow, since they will schedule some expensive work,
   * like Hadoop or Spark Job.
   *
   * If `Execution` is `FlatMapped` or `UniqueIdExecution`,
   * we are considering those executions as slow,
   * since we don't know which execution they can produce.
   *
   * Everything else we are considering as fast execution compare to `FlowDefExecution` and `WriteExecution`.
   */
  @tailrec
  def areFastExecution(es: List[Execution[Any]]): Boolean =
    es match {
      case Nil => true
      case h :: tail =>
        h match {
          case Execution.UniqueIdExecution(_) => false
          case Execution.FlowDefExecution(_) => false
          case Execution.WriteExecution(_, _, _) => false
          case Execution.FlatMapped(_, _) => false

          case Execution.ReaderExecution => areFastExecution(tail)
          case Execution.FutureConst(_) => areFastExecution(tail)
          case Execution.GetCounters(e) => areFastExecution(e :: tail)
          case Execution.ResetCounters(e) => areFastExecution(e :: tail)
          case Execution.WithNewCache(e) => areFastExecution(e :: tail)
          case Execution.TransformedConfig(e, _) => areFastExecution(e :: tail)
          case Execution.OnComplete(e, _) => areFastExecution(e :: tail)
          case Execution.RecoverWith(e, _) => areFastExecution(e :: tail)
          case Execution.Mapped(e, _) => areFastExecution(e :: tail)
          case Execution.Zipped(one, two) => areFastExecution(one :: two :: tail)
        }
    }

  object ZipWrite extends PartialRule[Execution] {
    type InputType = (Config, Mode, Execution.Writer, ConcurrentExecutionContext)
    type ResFun[T] =  InputType => Future[T]

    case class NewFn[T, U](left: ResFun[T], right: ResFun[U]) extends (InputType => Future[(T, U)]) {
      override def apply(tup: InputType): Future[(T, U)] =
        Execution.failFastZip(left(tup), right(tup))(tup._4)
    }

    override def applyWhere[T](on: Dag[Execution]) = {
      /*
      * run this and that in parallel, without any dependency. This will
      * be done in a single cascading flow if possible.
      *
      * If both sides are write executions then merge them
      */
      case Execution.Zipped(Execution.WriteExecution(oH, oT, oR), Execution.WriteExecution(tH, tT, tR)) =>
        Execution.WriteExecution(oH, tH :: tT ::: oT, NewFn(oR, tR))
    }
  }

  object ZipMap extends PartialRule[Execution] {
    case class MapLeft[S, T, B](fn: S => B) extends (((S, T)) => (B, T)) {
      override def apply(st: (S, T)): (B, T) = (fn(st._1), st._2)
    }

    case class MapRight[S, T, B](fn: T => B) extends (((S, T)) => (S, B)) {
      override def apply(st: (S, T)): (S, B) = (st._1, fn(st._2))
    }

    override def applyWhere[T](on: Dag[Execution]) = {
      case Execution.Zipped(Execution.Mapped(left, fn), right) =>
        Execution.Zipped(left, right).map(MapLeft(fn))
      case Execution.Zipped(left, Execution.Mapped(right, fn)) =>
        Execution.Zipped(left, right).map(MapRight(fn))
    }
  }

  object ZipFlatMap extends PartialRule[Execution] {
    case class LeftZipRight[S, T, B](left: Execution[B], fn: S => Execution[T]) extends (S => Execution[(B, T)]) {
      private val fun = fn.andThen(left.zip)

      override def apply(s: S): Execution[(B, T)] = fun(s)
    }

    case class RightZipLeft[S, T, B](right: Execution[B], fn: S => Execution[T]) extends (S => Execution[(T, B)]) {
      private val fun = fn.andThen(_.zip(right))

      override def apply(s: S): Execution[(T, B)] = fun(s)
    }

    case class NestedZip[S, T, B, A](right: Execution[B], lfn: S => Execution[T], rfn: B => Execution[A]) extends (S => Execution[(T, A)]) {
      private val fun = lfn.andThen { lr =>
        Execution.FlatMapped(right, rfn.andThen(lr.zip))
      }

      override def apply(s: S): Execution[(T, A)] = fun(s)
    }

    override def applyWhere[T](on: Dag[Execution]) = {
      case Execution.Zipped(Execution.FlatMapped(left, lfn), Execution.FlatMapped(right, rfn)) if isFastExecution(left) && isFastExecution(right) =>
        Execution.FlatMapped(left, NestedZip(right, lfn, rfn))
      case Execution.Zipped(Execution.FlatMapped(left, fn), right) if isFastExecution(left) =>
        Execution.FlatMapped(left, RightZipLeft(right, fn))
      case Execution.Zipped(left, Execution.FlatMapped(right, fn)) if isFastExecution(right) =>
        Execution.FlatMapped(right, LeftZipRight(left, fn))
    }
  }

  object MapWrite extends PartialRule[Execution] {
    override def applyWhere[T](on: Dag[Execution]) = {
      case Execution.Mapped(write@ Execution.WriteExecution(_, _, _), fn) =>
        write.map(fn)
    }
  }

  val std: Rule[Execution] =
    Rule.orElse(
      List(
        ZipWrite,
        MapWrite,
        ZipMap,
        ZipFlatMap
      )
    )

  def apply[A](e: Execution[A], r: Rule[Execution]): Execution[A] = {
    try {
      Dag.applyRule(e, toLiteral, r)
    } catch {
      case _: StackOverflowError => e
    }
  }

  def stdOptimizations[A](e: Execution[A]): Execution[A] =
    apply(e, std)
}
