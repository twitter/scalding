package com.twitter.scalding.hellcats

import cats.{Functor, FunctorFilter, MonoidK, Semigroupal, StackSafeMonad}
import cats.effect.{ Async, Effect, ExitCase, SyncIO, IO }
import com.twitter.scalding.{ Config, Mode, TypedPipe, Execution }
import com.twitter.scalding.typed.CoGroupable
import com.twitter.scalding.typed.functions.{Identity, MapOptionToFlatMap}
import scala.concurrent.{ Future, ExecutionContext => ConcurrentExecutionContext, Promise }

/**
 * Instances for cats types when working with Scalding
 */
object HellCats {
  implicit val instancesTypedPipe: Functor[TypedPipe] with MonoidK[TypedPipe] =
    new Functor[TypedPipe] with MonoidK[TypedPipe] {
      def empty[A] = TypedPipe.empty
      def map[A, B](ta: TypedPipe[A])(fn: A => B) = ta.map(fn)
      def combineK[A](left: TypedPipe[A], right: TypedPipe[A]) = left ++ right
      // we could impliment Applicative[TypedPipe], but cross is very dangerous
      // on map-reduce, so I hesitate to add it at this point
    }

  implicit val functorFilterTypedPipe: FunctorFilter[TypedPipe] =
    new FunctorFilter[TypedPipe] {
      def functor = instancesTypedPipe
      def mapFilter[A, B](ta: TypedPipe[A])(fn: A => Option[B]): TypedPipe[B] =
        ta.flatMap(MapOptionToFlatMap(fn))

      override def flattenOption[A](ta: TypedPipe[Option[A]]): TypedPipe[A] =
        mapFilter(ta)(Identity())

      override def collect[A, B](ta: TypedPipe[A])(fn: PartialFunction[A, B]): TypedPipe[B] =
        ta.collect(fn)

      override def filter[A](ta: TypedPipe[A])(fn: A => Boolean) = ta.filter(fn)
    }

  implicit def semigroupalCoGroupable[K]: Semigroupal[({type F[V] = CoGroupable[K, V]})#F] =
    new Semigroupal[({type F[V] = CoGroupable[K, V]})#F] {
      def product[A, B](ca: CoGroupable[K, A], cb: CoGroupable[K, B]) = ca.join(cb)
    }

  /**
   * Async[Execution] includes MonadError[Throwable, Execution] and Defer[Execution]
   * which together are the most commonly used typeclasses
   */
  implicit val asyncExecution: Async[Execution] with StackSafeMonad[Execution] =
    new AsyncExecution

  /**
   * To use Execution as an Effect, which is to say, we can run it, we need the Config, Mode
   * and ExecutionContext to use
   */
  def executionEffect(c: Config, m: Mode)(implicit cec: ConcurrentExecutionContext): Effect[Execution] =
    new ExecutionEffect(c, m)

  class AsyncExecution extends Async[Execution] with StackSafeMonad[Execution] {
    private[this] val neverNothing: Execution[Nothing] =
      Execution.fromFuture { _ =>
        val p = Promise[Nothing]()
        p.future
      }

    override def ap[A, B](ef: Execution[A => B])(ea: Execution[A]): Execution[B] =
      ef.zip(ea).map { case (f, a) => f(a) }

    def async[A](k: (Either[Throwable, A] => Unit) => Unit): Execution[A] =
      Execution.withNewCache(Execution.fromFuture { implicit cec: ConcurrentExecutionContext =>
        val p = Promise[A]()
        Future {
          k {
            case Right(a) =>
              p.success(a)
              ()
            case Left(err) =>
              p.failure(err)
              ()
          }
        }
        p.future
      })

    def asyncF[A](k: (Either[Throwable, A] => Unit) => Execution[Unit]): Execution[A] =
      Execution.withNewCache(Execution.fromFuture { implicit cec: ConcurrentExecutionContext =>
        val p = Promise[A]()
        Future {
          k {
            case Right(a) =>
              p.success(a)
              ()
            case Left(err) =>
              p.failure(err)
              ()
          }.map(_ => p)
        }
      }).flatten.flatMap { p => Execution.fromFuture(_ => p.future) }

    // Members declared in cats.effect.Bracket
    def bracketCase[A, B](acquire: Execution[A])(use: A => Execution[B])(release: (A, ExitCase[Throwable]) => Execution[Unit]): Execution[B] =
      acquire.flatMap { a =>
        attempt(use(a)).flatMap {
          case Right(b) =>
            release(a, ExitCase.Completed)
              .map(_ => b)
          case Left(t) =>
            release(a, ExitCase.Error(t))
              .flatMap(_ => Execution.failed(t))
        }
      }

    override def delay[A](a: => A): Execution[A] =
      // we can't lawfully cache this
      Execution.withNewCache(Execution.from(a))

    def handleErrorWith[A](ea: Execution[A])(fn: Throwable => Execution[A]): Execution[A] =
      ea.recoverWith { case t => fn(t) }

    def pure[A](a: A): Execution[A] = Execution.from(a)

    def flatMap[A, B](ea: Execution[A])(fn: A => Execution[B]): Execution[B] =
      ea.flatMap(fn)

    override def map[A, B](ea: Execution[A])(fn: A => B): Execution[B] =
      ea.map(fn)

    override def never[A]: Execution[A] = neverNothing

    override def product[A, B](ea: Execution[A], eb: Execution[B]): Execution[(A, B)] =
      ea.zip(eb)

    def raiseError[A](t: Throwable): Execution[A] = Execution.failed(t)

    override def recoverWith[A](ea: Execution[A])(fn: PartialFunction[Throwable, Execution[A]]): Execution[A] =
      ea.recoverWith(fn)

    def suspend[A](ea: => Execution[A]): Execution[A] =
      delay(ea).flatten
  }

  class ExecutionEffect(c: Config, m: Mode)(implicit cec: ConcurrentExecutionContext) extends AsyncExecution with Effect[Execution] {
    def runAsync[A](ea: Execution[A])(cb: Either[Throwable, A] => IO[Unit]): SyncIO[Unit] = {
      SyncIO {
        val funit = ea.run(c, m)
          .map { a => Right(a) }
          .recover { case t => Left(t) }
          .map { e => cb(e).unsafeRunSync }
        // we can discard this future, since we have started the work
        ()
      }
    }
  }
}
