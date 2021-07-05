package com.twitter.scalding.typed.gen

import com.twitter.scalding.Execution
import com.twitter.scalding.typed.TypedPipe
import org.scalacheck.{Cogen, Gen}

object ExecutionGen {
  import TypedPipeGen._

  private[this] def cogen(t: TypeWith[TypeGen]): Cogen[TypedPipe[t.Type]] =
    Cogen[TypedPipe[t.Type]] { pipe: TypedPipe[t.Type] =>
      pipe.hashCode.toLong
    }

  def executionOf(implicit tg: Gen[TypeWith[TypeGen]]): Gen[(Execution[TypedPipe[_]], TypeWith[TypeGen])] =
    tg.flatMap { t =>
      executionOf(t).map(_ -> t)
    }

  def executionOf(a: TypeWith[TypeGen])(implicit tg: Gen[TypeWith[TypeGen]]): Gen[Execution[TypedPipe[a.Type]]] =
    Gen.delay(
      Gen.frequency(
        5 -> genFrom(a),
        1 -> genMap(a),
        1 -> genFlatMap(a),
        1 -> tg.flatMap { t =>
          Gen.oneOf(
            genZipped(a, t).map(_.map(_._1)),
            genZipped(t, a).map(_.map(_._2))
          )
        }
      )
    )

  def genFrom(a: TypeWith[TypeGen])(implicit tg: Gen[TypeWith[TypeGen]]): Gen[Execution[TypedPipe[a.Type]]] =
    pipeOf(a).map(Execution.from(_))

  def genMap(a: TypeWith[TypeGen])(implicit tg: Gen[TypeWith[TypeGen]]): Gen[Execution[TypedPipe[a.Type]]] =
    tg.flatMap { t =>
      executionOf(t).flatMap { exec =>
        Gen.function1(pipeOf(a))(cogen(t)).map { f =>
          exec.map(f)
        }
      }
    }

  def genFlatMap(a: TypeWith[TypeGen])(implicit tg: Gen[TypeWith[TypeGen]]): Gen[Execution[TypedPipe[a.Type]]] =
    tg.flatMap { t =>
      executionOf(t).flatMap { exec =>
        Gen.function1(executionOf(a))(cogen(t)).map { f =>
          exec.flatMap(f)
        }
      }
    }

  def genZipped(l: TypeWith[TypeGen], r: TypeWith[TypeGen])(implicit tg: Gen[TypeWith[TypeGen]]): Gen[Execution[(TypedPipe[l.Type], TypedPipe[r.Type])]] =
    for {
      le <- executionOf(l)
      re <- executionOf(r)
    } yield le.zip(re)
}
