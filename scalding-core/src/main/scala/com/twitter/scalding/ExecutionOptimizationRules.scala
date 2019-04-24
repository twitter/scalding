package com.twitter.scalding

import com.stripe.dagon.{Dag, FunctionK, Literal, Memoize, PartialRule, Rule}
import com.twitter.scalding.typed.functions.{ ComposedFunctions, SubTypes, Swap }
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
     * expand graphs of WriteExecution, Zipped, Mapped and other into
     * a new dag of single, many, or mapped results. We do this
     * by keeping a Tuple (like a heterogeneous list) of all non-mapped
     * Executions. In this representation, we can recursively optimize.
     *
     * On this simpler dag, we define optimize that ensures that
     * after there is exactly 1 WriteExecution in the head position
     * in the Tuple
     *
     * The first type parameter Ex[x] will either be Execution[X]
     * or when optimized WriteExecution[X], proving that we have
     * pushed the Write into the head position of the Tuple
     */
    private sealed trait FlattenedZip[+Ex[x] <: Execution[x], +A] extends Serializable {
      import FlattenedZip._

      // this is an internal representation of Executions that when zipped give Execution[A]
      type ExH

      // here is value for the HList of Executions that have been unzipped
      def executions: ExH
      // this converts the above executions back into a single Execution, but still on the HList
      // type
      def toExecution(ex: ExH): Execution[A]

      // convert back to the original Execution type
      final def execution: Execution[A] =
        toExecution(executions)

      def zip[B](that: FlattenedZip[Execution, B]): FlattenedZip[Execution, (A, B)]
      final def map[B](fn: A => B): FlattenedZip[Ex, B] =
        this match {
          case MapZip(f1, fn1) =>
            MapZip(f1, ComposedFunctions.ComposedMapFn(fn1, fn))
          case notMap =>
            MapZip(notMap, fn)
        }

      /**
       * How many writes are there in this FlattenedZip
       * if this number is 2 or more, the optimization can apply
       */
      final def writeCount: Int = {
        def isW[A](ex: Execution[A]): Int =
          ex match {
            case WriteExecution(_, _, _) => 1
            case _ => 0
          }
        @annotation.tailrec
        def loop(fz: FlattenedZip[Execution, Any], acc: Int): Int =
          fz match {
            case Single(w) => acc + isW(w)
            case Many(h, rest, _) => loop(rest, acc + isW(h))
            case MapZip(of, _) => loop(of, acc)
          }

        loop(this, 0)
      }

      /**
       * The type of this ensures that the head of the HList
       * must be a WriteExecution. Additionally, none of
       * the other items are, but we don't prove that in the types,
       * instead we prove that with tests
       *
       * Note, one might imagine we could always return a FlattenedZip
       * here, if the optimization does not apply, just return `this`.
       * That intuition is not quite correct: the type changes in the result
       * Since we start as FlattenedZip[Ex, A] for some Ex[A] subtype of
       * Execution[A] but we return
       * FlattenedZip[WriteExecution, A], if there are no WriteExecution
       * instances in the graph, we have no way of returning a FlattenedZip
       * with one in it.
       *
       * It's a bit confusing because below when we use it in our rule,
       * we only call it when we know it applies since we count
       * the writes and verify there are 2 or more, but internally
       * we recursively call this method, and we do exercise the None
       * branch.
       */
      def optimize: Option[FlattenedZip[WriteExecution, A]]
    }

    private object FlattenedZip {

      /**
       * Here is the simplest FlattenZip: just a single Execution
       */
      final case class Single[+Ex[x] <: Execution[x], A](ex: Ex[A]) extends FlattenedZip[Ex, A] {
        type ExH = Execution[A]
        def executions = ex
        def toExecution(exa: Execution[A]): Execution[A] =
          exa

        def zip[B](that: FlattenedZip[Execution, B]) =
          that match {
            case s@Single(ex2) => Many[Execution, A, B](ex, s)
            case m@Many(_, _, _) =>
              def go[X, Y](m: Many[Execution, X, Y, B]): FlattenedZip[Execution, (A, B)] = {
                val sub0: SubTypes[(X, Y), B] = m.sub
                val flat0: FlattenedZip[Execution, (A, (X, Y))] =
                  Many[Execution, A, (X, Y)](ex, Single(m.head).zip(m.rest))
                type F[+X] = FlattenedZip[Execution, (A, X)]
                sub0.liftCo[F](flat0)
              }
              go(m)
            case MapZip(left, fn) =>
              zip(left).map(ZipMap.MapRight(fn))
          }
        def optimize: Option[FlattenedZip[WriteExecution, A]] =
          ex match {
            case w@WriteExecution(_, _, _) => Some(Single(w))
            case _ => None
          }
      }

      /**
       * Here is a non-empty list of Executions. Since scala has a hard time with inferring types
       * of ADTs we use the Liskov trick here, which we call SubTypes in the scalding codebase.
       * This allows us to prove without casts when needed that (A, T) <: C which
       * is something scala has a hard time with.
       */
      final case class Many[+Ex[x] <: Execution[x], A, T, +C](head: Ex[A], rest: FlattenedZip[Execution, T], sub: SubTypes[(A, T), C]) extends FlattenedZip[Ex, C] {
        type ExH = (Execution[A], rest.ExH)
        def executions = (head, rest.executions)
        def toExecution(ex: (Execution[A], rest.ExH)): Execution[C] =
          sub.liftCo[Execution](Zipped(ex._1, rest.toExecution(ex._2)))

        def zip[B](that: FlattenedZip[Execution, B]): FlattenedZip[Execution, (C, B)] = {
          val m1: FlattenedZip[Execution, (A, (T, B))] = Many(head, rest.zip(that))
          val m2: FlattenedZip[Execution, ((A, T), B)] = m1.map(UnTwist())
          type FZ[+X] = FlattenedZip[Execution, (X, B)]
          sub.liftCo[FZ](m2)
        }

        private def fix[Ex[x] <: Execution[x]](f: FlattenedZip[Ex, (A, T)]): FlattenedZip[Ex, C] = {
          type FZ[+X] = FlattenedZip[Ex, X]
          sub.liftCo[FZ](f)
        }

        /**
         * This is the meat of the zip optimization. Unfortunaely we case out all 8 possibilities:
         * 1. head may or may not be a write.
         * 2. tail may not be optimizable (None) or it may optimize returning any 3 subclasses
         * that gives 2 x 4 possibilities to deal with
         */
        def optimize: Option[FlattenedZip[WriteExecution, C]] =
          head match {
            // First we consider the case where head is already a write
            case headW@WriteExecution(h, t, fn) =>
              rest.optimize match {
                case None =>
                  // the head is already the only Write
                  // if not, rest could be optimized
                  Some(Many(headW, rest, sub))
                case Some(Single(w)) =>
                  // Single is fully optimized
                  Some(fix(Single(mergeWrite(headW, w))))
                case Some(m@Many(_, _, _)) =>
                  def go[X, Y](m: Many[WriteExecution, X, Y, T]): FlattenedZip[WriteExecution, (A, T)] = {
                    // we know that (X, Y) <: T
                    // here we combine the write in the head of rest, with the outer head
                    val combine: WriteExecution[(A, X)] = mergeWrite(headW, m.head)
                    // now we push that write back on the head of the optimized rest (which
                    // by invariant, cannot contain a Write
                    val many2: Many[WriteExecution, (A, X), Y, ((A, X), Y)] = Many(combine, m.rest)
                    // how we have to fix the types up
                    val f1: FlattenedZip[WriteExecution, (A, (X, Y))] = MapZip(many2, Twist[A, X, Y]())
                    type F[+Z] = FlattenedZip[WriteExecution, (A, Z)]
                    m.sub.liftCo[F](f1)
                  }
                  Some(fix(go(m)))
                case Some(m@MapZip(_, _)) =>
                  // here we just recurse without the mapFn, since removing the mapFn
                  // reduces the size of the dag, we are making progress on the recursion
                  def go[X](m: MapZip[WriteExecution, X, T]): Option[FlattenedZip[WriteExecution, (A, T)]] = {
                    Many(headW, m.flat)
                      .optimize
                      .map(_.map(ZipMap.MapRight(m.mapFn)))
                  }

                  go(m).map(fix(_))
              }
            case notWrite =>
              rest.optimize match {
                case None =>
                  // there are no writes
                  None
                case Some(Single(w)) =>
                  // put the w, which is a write, at the head, and the notWrite behind
                  val swap: Many[WriteExecution, T, A, (T, A)] = Many(w, Single(notWrite))
                  // but now the types are wrong, so we need to swap
                  Some(fix(swap.map(Swap())))
                case Some(m@Many(_, _, _)) =>
                  def go[X, Y](m: Many[WriteExecution, X, Y, T]): FlattenedZip[WriteExecution, (A, T)] = {
                    // we know that (X, Y) <: T
                    // first put the write at the head, and push the old head into the second
                    // position
                    val many2: Many[WriteExecution, X, (A, Y), (X, (A, Y))] =
                      Many(m.head, Many(notWrite, m.rest))
                    // use TwistSwap to fix the types back up
                    val twisted: FlattenedZip[WriteExecution, (A, (X, Y))] =
                      MapZip(many2, TwistSwap[X, A, Y]())
                    // substitute to get back to the (A, T) type finally
                    type F[+Z] = FlattenedZip[WriteExecution, (A, Z)]
                    m.sub.liftCo[F](twisted)
                  }
                  Some(fix(go(m)))
                case Some(m@MapZip(_, _)) =>
                  // here we just recurse without the mapFn, since removing the mapFn
                  // reduces the size of the dag, we are making progress on the recursion
                  def go[X](m: MapZip[WriteExecution, X, T]): Option[FlattenedZip[WriteExecution, (A, T)]] = {
                    Many(notWrite, m.flat)
                      .optimize
                      .map(_.map(ZipMap.MapRight(m.mapFn)))
                  }

                  go(m).map(fix(_))
              }
          }
      }

      object Many {
        /**
         * This is a handy constructor function that builds the SubTypes instance for us
         */
        def apply[Ex[x] <: Execution[x], A, B](head: Ex[A], rest: FlattenedZip[Execution, B]): Many[Ex, A, B, (A, B)] =
          Many(head, rest, SubTypes.fromSubType[(A, B), (A, B)])
      }

      /**
       * This represents pushing all the map functions after the zips, this is needed to
       * handle the internal mapping functions we do to reverse the order of zips and get
       * the WriteExecution moves all the way over to the head
       */
      final case class MapZip[Ex[x] <: Execution[x], A, B](flat: FlattenedZip[Ex, A], mapFn: A => B) extends FlattenedZip[Ex, B] {
        type ExH = flat.ExH

        def executions: ExH = flat.executions
        def toExecution(ex: ExH): Execution[B] =
          Mapped(flat.toExecution(ex), mapFn)

        def zip[C](that: FlattenedZip[Execution, C]): FlattenedZip[Execution, (B, C)] =
          MapZip(flat.zip(that), ZipMap.MapLeft[A, C, B](mapFn))

        def optimize: Option[FlattenedZip[WriteExecution, B]] =
          flat.optimize.map(MapZip(_, mapFn))
      }

      /**
       * Convert an Execution to the Flattened (tuple-ized) representation
       */
      def apply[A](ex: Execution[A]): FlattenedZip[Execution, A] =
        ex match {
          case Zipped(left, right) => apply(left).zip(apply(right))
          case Mapped(that, fn) => apply(that).map(fn)
          case notZipMap => FlattenedZip.Single(notZipMap)
        }
    }

    /**
     * Apply the optimization of merging all zipped/mapped WriteExecution
     * into a single value. If ex is already optimal (0 or 1 write) return None
     */
    def optimize[A](ex: Execution[A]): Option[Execution[A]] = {
      val flat = FlattenedZip(ex)
      val wc = flat.writeCount
      // only optimize if there are 2 or more writes, otherwise we create an infinite loop
      if (wc > 1) {
        // unless there is a bug, this optimized will be defined
        // but internally, optimize can return None. Just to be
        // cleaner we use flatMap here rather than getOrElse(sys.error("unreachable"))
        flat.optimize.map(_.execution)
      }
      else None
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
