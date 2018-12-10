package com.twitter.scalding.hellcats

import cats.{ Eq, MonadError }
import cats.laws.discipline.SemigroupalTests.Isomorphisms
import cats.effect.{ Effect, IO }
import cats.effect.laws.discipline.EffectTests
import com.twitter.scalding.typed.memory_backend.MemoryMode
import com.twitter.scalding.{ Execution, Config }
import org.scalatest.FunSuite
import org.scalacheck.{ Arbitrary, Gen }
import org.typelevel.discipline.scalatest.Discipline
import scala.concurrent.{ Await, ExecutionContext }
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

import HellCats._
import cats.implicits._

object ExecutionGen {
  def genMonadError[F[_], A](depth: Int, g: Gen[A])(implicit me: MonadError[F, Throwable]): Gen[F[A]] = {
    val recurse = Gen.lzy(genMonadError[F, A](depth - 1, g))
    val g0 = Gen.frequency((5, g.map(me.pure(_))), (1, Gen.const(me.raiseError[A](new Exception("failed")))))
    if (depth <= 0) g0
    else {
      implicit val arbEx: Arbitrary[F[A]] = Arbitrary(recurse)
      val genFn = Arbitrary.arbitrary[Int => F[A]]
      val genIntEx = Gen.lzy(genMonadError[F, Int](depth - 1, Arbitrary.arbitrary[Int]))
      val genFlatMap = for {
        ei <- genIntEx
        fn <- genFn
      } yield ei.flatMap(fn)
      val zip = for {
        a <- recurse
        b <- recurse
        aOrB <- Gen.oneOf(a, b)
      } yield aOrB

      Gen.frequency(
        (4, g0),
        (4, genFlatMap),
        (1, zip)) // use zip less because it branches
    }
  }
  def genExecution[A](depth: Int, g: Gen[A]): Gen[Execution[A]] =
    genMonadError[Execution, A](depth, g)

  implicit def arbEx[A](implicit arb: Arbitrary[A]): Arbitrary[Execution[A]] =
    Arbitrary(genExecution(5, arb.arbitrary))

  implicit def arbIO[A](implicit arb: Arbitrary[A]): Arbitrary[IO[A]] =
    Arbitrary(genMonadError[IO, A](5, arb.arbitrary))

  implicit def eqEx[A: Eq](implicit ec: ExecutionContext): Eq[Execution[A]] =
    new Eq[Execution[A]] {
      def get[A](ex: Execution[A]): Try[A] =
        Try(Await.result(ex.run(Config.empty, MemoryMode.empty), Duration(10, SECONDS)))

      def eqv(l: Execution[A], r: Execution[A]) =
        (get(l), get(r)) match {
          case (Success(a), Success(b)) => Eq[A].eqv(a, b)
          case (Failure(_), Failure(_)) => true
          case _ => false
        }
    }

  implicit def eqIO[A: Eq]: Eq[IO[A]] =
    new Eq[IO[A]] {
      def eqv(l: IO[A], r: IO[A]) =
        (Try(l.unsafeRunTimed(Duration(10, SECONDS))),
          Try(r.unsafeRunTimed(Duration(10, SECONDS)))) match {
            case (Success(a), Success(b)) => Eq[Option[A]].eqv(a, b)
            case (Failure(_), Failure(_)) => true
            case _ => false
          }
    }

  // We consider all failures the same, we don't care about failure order
  // in Execution because we want to fail fast
  implicit val allEqThrowable: Eq[Throwable] =
    Eq.by { t: Throwable => () }

  implicit val isos: Isomorphisms[Execution] = Isomorphisms.invariant[Execution]
  // Need non-fatal Throwables for Future recoverWith/handleError
  implicit val nonFatalArbitrary: Arbitrary[Throwable] =
    Arbitrary(Arbitrary.arbitrary[Exception].map(identity))
}

class HellCatsTests extends FunSuite with Discipline {
  import ExecutionGen._

  implicit val ec: ExecutionContext = ExecutionContext.global

  {
    implicit val exeEff: Effect[Execution] = executionEffect(Config.empty, MemoryMode.empty)
    checkAll("Execution", EffectTests[Execution].effect[Int, Int, Int])
  }
}
