package com.twitter.scalding.typed.gen

import com.twitter.scalding.TypedPipe
import org.scalacheck.{Cogen, Gen}

object TypedPipeGen {
  def pipeOf(implicit tg: Gen[TypeWith[TypeGen]]): Gen[(TypedPipe[_], TypeWith[TypeGen])] =
    tg.flatMap { t =>
      pipeOf(t).map(_ -> t)
    }

  def pipeOf(a: TypeWith[TypeGen])(implicit tg: Gen[TypeWith[TypeGen]]): Gen[TypedPipe[a.Type]] =
    Gen.delay(
      Gen.frequency(
        5 -> genFrom(a),
        1 -> genFork(a),
        1 -> genForceToDisk(a),
        1 -> genDistinct(a),
        1 -> genFilter(a),
        1 -> genCollect(a),
        1 -> genMap(a),
        1 -> genFlatMap(a),
        1 -> genMerge(a),
        1 -> tg.flatMap { t =>
          Gen.oneOf(
            pipeOf(a, t).map(_.keys),
            pipeOf(t, a).map(_.values)
          )
        },
        1 -> Gen.zip(tg, tg).flatMap { case (t1, t2) =>
          Gen.oneOf(
            pipeOf(a, t1, t2).map(_.keys),
            pipeOf (t1, a, t2).map(_.values.keys),
            pipeOf (t1, t2, a).map(_.values.values)
          )
        }
      )
    )

  def pipeOf(k: TypeWith[TypeGen], v: TypeWith[TypeGen])(implicit tg: Gen[TypeWith[TypeGen]]): Gen[TypedPipe[(k.Type, v.Type)]] =
    Gen.delay(
      Gen.oneOf(
        genGrouped(k, v),
        genSortedGrouped(k, v),
        genWithReducers(k, v),
        genMapGroup(k, v),
        genFilterKeys(k, v),
        genMapValues(k, v),
        genFlatMapValues(k, v),
        genSumByKey(k, v),
        genSumByLocalKeys(k, v),
        genCross(k, v)
      )
    )

  def pipeOf(
    k: TypeWith[TypeGen],
    v1: TypeWith[TypeGen],
    v2: TypeWith[TypeGen]
  )(implicit tg: Gen[TypeWith[TypeGen]]): Gen[TypedPipe[(k.Type, (v1.Type, v2.Type))]] =
    Gen.delay(
      Gen.oneOf(
        genJoin(k, v1, v2),
        genHashJoin(k, v1, v2)
      )
    )

  def genFrom(a: TypeWith[TypeGen]): Gen[TypedPipe[a.Type]] =
    for {
      lst <- Gen.listOf(a.evidence.gen)
    } yield TypedPipe.from(lst)

  def genFork(a: TypeWith[TypeGen])(implicit tg: Gen[TypeWith[TypeGen]]): Gen[TypedPipe[a.Type]] =
    pipeOf(a).map(_.fork)

  def genForceToDisk(a: TypeWith[TypeGen])(implicit tg: Gen[TypeWith[TypeGen]]): Gen[TypedPipe[a.Type]] =
    pipeOf(a).map(_.forceToDisk)

  def genDescription(a: TypeWith[TypeGen])(implicit tg: Gen[TypeWith[TypeGen]]): Gen[TypedPipe[a.Type]] =
    Gen.identifier.flatMap { desc =>
      pipeOf(a).map(_.withDescription(desc))
    }

  def genMap(a: TypeWith[TypeGen])(implicit tg: Gen[TypeWith[TypeGen]]): Gen[TypedPipe[a.Type]] =
    tg.flatMap { b =>
      pipeOf(b).flatMap { pipe =>
        Gen.function1(a.evidence.gen)(b.evidence.cogen).map { f =>
          pipe.map(f)
        }
      }
    }

  def genFilter(a: TypeWith[TypeGen])(implicit tg: Gen[TypeWith[TypeGen]]): Gen[TypedPipe[a.Type]] =
    pipeOf(a).flatMap { pipe =>
      Gen.function1(StdGen.booleanGen)(a.evidence.cogen).map { f =>
        pipe.filter(f)
      }
    }

  def genCollect(a: TypeWith[TypeGen])(implicit tg: Gen[TypeWith[TypeGen]]): Gen[TypedPipe[a.Type]] =
    tg.flatMap { b =>
      pipeOf(b).flatMap { pipe =>
        Gen.zip(
          Gen.function1(StdGen.booleanGen)(b.evidence.cogen),
          Gen.function1(a.evidence.gen)(b.evidence.cogen)
        ).flatMap { case (p, f) =>
          val pf = new PartialFunction[b.Type, a.Type] {
            override def isDefinedAt(x: b.Type): Boolean = p.apply(x)

            override def apply(v1: b.Type): a.Type = f.apply(v1)
          }

          pipe.collect(pf)
        }
      }
    }

  def genFlatMap(a: TypeWith[TypeGen])(implicit tg: Gen[TypeWith[TypeGen]]): Gen[TypedPipe[a.Type]] =
    tg.flatMap { b =>
      pipeOf(b).flatMap { pipe =>
        Gen.function1(Gen.listOf(a.evidence.gen))(b.evidence.cogen).map { f =>
          pipe.flatMap(f)
        }
      }
    }

  def genMerge(a: TypeWith[TypeGen])(implicit tg: Gen[TypeWith[TypeGen]]): Gen[TypedPipe[a.Type]] =
    for {
      left <- pipeOf(a)
      right <- pipeOf(a)
    } yield left ++ right

  def genDistinct(a: TypeWith[TypeGen])(implicit tg: Gen[TypeWith[TypeGen]]): Gen[TypedPipe[a.Type]] =
    pipeOf(a).map { pipe =>
      pipe.distinct(a.evidence.ordering)
    }

  def genGrouped(
    k: TypeWith[TypeGen],
    v: TypeWith[TypeGen]
  )(implicit tg: Gen[TypeWith[TypeGen]]): Gen[TypedPipe[(k.Type, v.Type)]] =
    pipeOf(
      TypeWith(TypeGen(k.evidence, v.evidence))
    ).map { pipe =>
      pipe.group(k.evidence.ordering)
    }

  def genSortedGrouped(
    k: TypeWith[TypeGen],
    v: TypeWith[TypeGen]
  )(implicit tg: Gen[TypeWith[TypeGen]]): Gen[TypedPipe[(k.Type, v.Type)]] =
    pipeOf(k, v).map { pipe =>
      pipe.group(k.evidence.ordering).sorted(v.evidence.ordering)
    }

  def genWithReducers(
    k: TypeWith[TypeGen],
    v: TypeWith[TypeGen]
  )(implicit tg: Gen[TypeWith[TypeGen]]): Gen[TypedPipe[(k.Type, v.Type)]] =
    pipeOf(k, v).flatMap { pipe =>
      StdGen.intGen.map { reducers =>
        pipe.group(k.evidence.ordering).withReducers(reducers)
      }
    }

  def genMapGroup(
    k: TypeWith[TypeGen],
    v: TypeWith[TypeGen]
  )(implicit tg: Gen[TypeWith[TypeGen]]): Gen[TypedPipe[(k.Type, v.Type)]] =
    tg.flatMap { t =>
      def cogenIter[A: Cogen]: Cogen[Iterator[A]] =
        Cogen.it(_.toIterator)

      pipeOf(k, t).flatMap { pipe =>
        Gen.function1(
          Gen.listOf(v.evidence.gen).map(_.toIterator)
        )(
          Cogen.tuple2(k.evidence.cogen, cogenIter(t.evidence.cogen))
        ).map { f =>
          pipe.group(k.evidence.ordering).mapGroup(Function.untupled(f)).toTypedPipe
        }
      }
    }

  def genMapValues(
    k: TypeWith[TypeGen],
    v: TypeWith[TypeGen]
  )(implicit tg: Gen[TypeWith[TypeGen]]): Gen[TypedPipe[(k.Type, v.Type)]] =
    tg.flatMap { t =>
      pipeOf(k, t).flatMap { pipe =>
        Gen.function1(v.evidence.gen)(t.evidence.cogen).map { f =>
          pipe.mapValues(f)
        }
      }
    }

  def genFlatMapValues(
    k: TypeWith[TypeGen],
    v: TypeWith[TypeGen]
  )(implicit tg: Gen[TypeWith[TypeGen]]): Gen[TypedPipe[(k.Type, v.Type)]] =
    tg.flatMap { t =>
      pipeOf(k, t).flatMap { pipe =>
        Gen.function1(Gen.listOf(v.evidence.gen))(t.evidence.cogen).map { f =>
          pipe.flatMapValues(f)
        }
      }
    }

  def genFilterKeys(
    k: TypeWith[TypeGen],
    v: TypeWith[TypeGen]
  )(implicit tg: Gen[TypeWith[TypeGen]]): Gen[TypedPipe[(k.Type, v.Type)]] =
    pipeOf(k, v).flatMap { pipe =>
      Gen.function1(StdGen.booleanGen)(k.evidence.cogen).map { f =>
        pipe.filterKeys(f)
      }
    }

  def genSumByKey(
    k: TypeWith[TypeGen],
    v: TypeWith[TypeGen]
  )(implicit tg: Gen[TypeWith[TypeGen]]): Gen[TypedPipe[(k.Type, v.Type)]] =
    pipeOf(k, v).flatMap { pipe =>
      pipe.sumByKey(k.evidence.ordering, v.evidence.semigroup).toTypedPipe
    }

  def genSumByLocalKeys(
    k: TypeWith[TypeGen],
    v: TypeWith[TypeGen]
  )(implicit tg: Gen[TypeWith[TypeGen]]): Gen[TypedPipe[(k.Type, v.Type)]] =
    pipeOf(k, v).flatMap { pipe =>
      pipe.sumByLocalKeys(v.evidence.semigroup)
    }

  def genJoin(
    k: TypeWith[TypeGen],
    v1: TypeWith[TypeGen],
    v2: TypeWith[TypeGen]
  )(implicit tg: Gen[TypeWith[TypeGen]]): Gen[TypedPipe[(k.Type, (v1.Type, v2.Type))]] =
    for {
      g1 <- pipeOf(k, v1)
      g2 <- pipeOf(k, v2)
    } yield g1.group(k.evidence.ordering) join g2.group(k.evidence.ordering)

  def genHashJoin(
    k: TypeWith[TypeGen],
    v1: TypeWith[TypeGen],
    v2: TypeWith[TypeGen]
  )(implicit tg: Gen[TypeWith[TypeGen]]): Gen[TypedPipe[(k.Type, (v1.Type, v2.Type))]] =
    for {
      g1 <- pipeOf(k, v1)
      g2 <- pipeOf(k, v2)
    } yield g1.group(k.evidence.ordering) hashJoin g2.group(k.evidence.ordering)

  def genCross(a: TypeWith[TypeGen], b: TypeWith[TypeGen])(implicit tg: Gen[TypeWith[TypeGen]]): Gen[TypedPipe[(a.Type, b.Type)]] =
    Gen.zip(pipeOf(a), pipeOf(b)).map { case (p1, p2) =>
      p1.cross(p2)
    }
}
