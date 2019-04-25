package com.twitter.scalding

import com.stripe.dagon.{Dag, FunctionK, Literal, Memoize, PartialRule, Rule}
import com.twitter.scalding.ExecutionOptimizationRules.ZipMap.{MapLeft, MapRight}
import com.twitter.scalding.typed.functions.ComposedFunctions.ComposedMapFn
import com.twitter.scalding.typed.functions.{ComposedFunctions, Identity, Swap}
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
    areFastExecution(e :: Nil)

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

  /**
   * This is a rather complex optimization rule, but also very important.
   * After this runs, there will only be 1 WriteExecution in a graph,
   * other than within recoverWith/flatMap/uniqueId nodes.
   *
   * This is the best we can do without running those functions.
   * The motivation for this is to allow the user to write Executions
   * as is convenient in code, but still have full access to a TypedPipe
   * graph when planning a stage. Without this, we can wind up recomputing
   * work that we don't need to do.
   */
  case object ZipWrite extends Rule[Execution] {
    import Execution._

    /*
     * First we define some case class functions to make sure
     * the rule is reproducible and doesn't break equality
     */
    case class Twist[A, B, C]() extends Function1[((A, B), C), (A, (B, C))] {
      def apply(in: ((A, B), C)) =
        (in._1._1, (in._1._2, in._2))
    }
    case class UnTwist[A, B, C]() extends Function1[(A, (B, C)), ((A, B), C)] {
      def apply(in: (A, (B, C))) =
        ((in._1, in._2._1), in._2._2)
    }
    case class TwistSwap[A, B, C]() extends Function1[(A, (B, C)), (B, (A, C))] {
      def apply(in: (A, (B, C))) =
        (in._2._1, (in._1, in._2._2))
    }
    case class ComposedFn[A1, A2, A, B1, B2, B](
      fn1: Function1[(A1, A2), A],
      fn2: Function1[(B1, B2), B]
    ) extends Function1[((A1, B1), (A2, B2)), (A, B)] {
      override def apply(v1: ((A1, B1), (A2, B2))): (A, B) = (fn1(v1._1._1, v1._2._1), fn2(v1._1._2, v1._2._2))
    }

    case class ComposeWriteFn[A, B, C, D, E](
      fn1: ((A, B, C, ConcurrentExecutionContext)) => Future[D],
      fn2: ((A, B, C, ConcurrentExecutionContext)) => Future[E]) extends Function1[(A, B, C, ConcurrentExecutionContext), Future[(D, E)]] {

      def apply(tup: (A, B, C, ConcurrentExecutionContext)): Future[(D, E)] =
        (Execution.failFastZip(fn1(tup), fn2(tup))(tup._4))
    }

    def mergeWrite[A, B](w1: WriteExecution[A], w2: WriteExecution[B]): WriteExecution[(A, B)] = {
      val newFn = ComposeWriteFn(w1.result, w2.result)
      WriteExecution(w1.head, w1.tail ::: (w2.head :: w2.tail), newFn)
    }

    /**
     * This is the fundamental type we use to optimize zips, basically we
     * expand graphs of WriteExecution, Zipped, Mapped.
     * Our goal to optimize any `Execution`'s DAG to have at most one write.
     *
     * This is achieved by optimizing any `Execution` to either:
     * - `NonWrite` execution
     * - `Write` execution
     * - composed execution which has both write and non write.
     */
    private sealed trait FlattenedZip[+A]

    private object FlattenedZip {
      final case class NonWrite[T](nonWrite: Execution[T]) extends FlattenedZip[T]
      final case class Write[T](write: WriteExecution[T]) extends FlattenedZip[T]
      final case class Composed[T1, T2, T](write: WriteExecution[T1], nonWrite: Execution[T2], compose: Function1[(T1, T2), T]) extends FlattenedZip[T]

      def toExecution[A](ex: FlattenedZip[A]): Execution[A] = ex match {
        case NonWrite(nonWrite) => nonWrite
        case Write(write) => write
        case c@Composed(_, _, _) => c.write.zip(c.nonWrite).map(c.compose)
      }

      def map[A, B](ex: FlattenedZip[A], fn: A => B): FlattenedZip[B] = ex match {
        case NonWrite(nonWrite) =>
          NonWrite(nonWrite.map(fn))
        case Write(write) =>
          Write(WriteExecution(write.head, write.tail, MapWrite.ComposeMap(write.result, fn)))
        case Composed(write, nonWrite, compose) =>
          Composed(write, nonWrite, ComposedMapFn(compose, fn))
      }

      def zip[A, B](left: FlattenedZip[A], right: FlattenedZip[B]): FlattenedZip[(A, B)] = (left, right) match {
        case (left@NonWrite(_), right@NonWrite(_)) =>
          NonWrite(left.nonWrite.zip(right.nonWrite))
        case (left@NonWrite(_), right@Write(_)) =>
          Composed(right.write, left.nonWrite, Swap[B, A]())
        case (left@NonWrite(_), right@Composed(_, _, _)) =>
          zipNonWriteComposed(left, right)

        case (left@Write(_), right@NonWrite(_)) =>
          Composed(left.write, right.nonWrite, Identity[(A, B)]())
        case (left@Write(_), right@Write(_)) =>
          Write(mergeWrite(left.write, right.write))
        case (left@Write(_), right@Composed(_, _, _)) =>
          zipWriteComposed(left, right)

        case (left@Composed(_, _, _), right@NonWrite(_)) =>
          map(zipNonWriteComposed(right, left), Swap[B, A]())
        case (left@Composed(_, _, _), right@Write(_)) =>
          map(zipWriteComposed(right, left), Swap[B, A]())
        case (left@Composed(_, _, _), right@Composed(_, _, _)) =>
          Composed(mergeWrite(left.write, right.write), left.nonWrite.zip(right.nonWrite),
            ComposedFn(left.compose, right.compose))
      }

      private def zipNonWriteComposed[A, B1, B2, B](left: NonWrite[A], right: Composed[B1, B2, B]): Composed[B1, (B2, A), (A, B)] =
        Composed(right.write, right.nonWrite.zip(left.nonWrite),
          ComposedMapFn(ComposedMapFn(UnTwist(), MapLeft[(B1, B2), A, B](right.compose)), Swap[B, A]()))

      private def zipWriteComposed[A, B1, B2, B](left: Write[A], right: Composed[B1, B2, B]): Composed[(A, B1), B2, (A, B)] =
        Composed(mergeWrite(left.write, right.write), right.nonWrite,
          ComposedMapFn(Twist(), MapRight[A, (B1, B2), B](right.compose)))

      /**
       * Convert an Execution to the Flattened (tuple-ized) representation
       */
      def apply[A](ex: Execution[A]): FlattenedZip[A] =
        ex match {
          case Zipped(left, right) => zip(apply(left), apply(right))
          case Mapped(that, fn) => map(apply(that), fn)
          case write@WriteExecution(_, _, _) => FlattenedZip.Write(write)
          case notZipMap => FlattenedZip.NonWrite(notZipMap)
        }
    }

    /**
     * Apply the optimization of merging all zipped/mapped WriteExecution
     * into a single value. If ex is already optimal (0 or 1 write) return None
     */
    def optimize[A](ex: Execution[A]): Option[Execution[A]] = {
      def writes(execution: Execution[_]): Int = {
        @tailrec
        def loop(executions: List[Execution[_]], acc: Int): Int = executions match {
          case Nil => acc
          case head :: tail => head match {
            case Zipped(left, right) => loop(left :: right :: tail, acc)
            case Mapped(that, _) => loop(that :: tail, acc)
            case WriteExecution(_, _, _) => loop(tail, acc + 1)
            case _ => loop(tail, acc)
          }
        }
        loop(execution :: Nil, 0)
      }
      // only optimize if there are 2 or more writes, otherwise we create an infinite loop
      if (writes(ex) > 1)
        Some(FlattenedZip.toExecution(FlattenedZip(ex)))
      else
        None
    }

    def apply[A](on: Dag[Execution]) = {
      case z@Zipped(_, _) => optimize(z)
      case _ =>
        // since this optimization only applies to zips, there
        // is no need to check on nodes that aren't zips.
        None
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
    case class ComposeMap[A, B, C, D, E](
      fn1: ((A, B, C, ConcurrentExecutionContext)) => Future[D],
      fn2: D => E) extends Function1[(A, B, C, ConcurrentExecutionContext), Future[E]] {

      def apply(tup: (A, B, C, ConcurrentExecutionContext)): Future[E] =
        fn1(tup).map(fn2)(tup._4)
    }

    override def applyWhere[T](on: Dag[Execution]) = {
      case Execution.Mapped(Execution.WriteExecution(h, t, f1), f2) =>
        Execution.WriteExecution(h, t, ComposeMap(f1, f2))
    }
  }

  case object FuseMaps extends PartialRule[Execution] {
    import Execution._
    def applyWhere[A](on: Dag[Execution]) = {
      case Mapped(Mapped(ex, fn0), fn1) =>
        Mapped(ex, ComposedFunctions.ComposedMapFn(fn0, fn1))
    }
  }


  val std: Rule[Execution] =
    Rule.orElse(
      List(
        ZipWrite,
        MapWrite,
        ZipMap,
        ZipFlatMap,
        FuseMaps
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
