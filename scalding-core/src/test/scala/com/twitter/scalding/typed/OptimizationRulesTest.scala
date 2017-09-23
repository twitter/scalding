package com.twitter.scalding.typed

import com.twitter.scalding.source.TypedText
import org.scalatest.FunSuite
import org.scalatest.prop.PropertyChecks
import org.scalacheck.{ Arbitrary, Gen }
import PropertyChecks.forAll

object TypedPipeGen {
  val srcGen: Gen[TypedPipe[Int]] = {
    val g1 = Gen.listOf(Arbitrary.arbitrary[Int]).map(TypedPipe.from(_))
    val src = Gen.identifier.map { f => TypedPipe.from(TypedText.tsv[Int](f)) }
    Gen.oneOf(g1, src, Gen.const(TypedPipe.empty))
  }

  lazy val mapped: Gen[TypedPipe[Int]] = {
    val next1: Gen[TypedPipe[Int] => TypedPipe[Int]] =
      Gen.oneOf(
        tpGen.map { p: TypedPipe[Int] =>
          { x: TypedPipe[Int] => x.cross(p).keys }
        },
        tpGen.map { p: TypedPipe[Int] =>
          { x: TypedPipe[Int] => x.cross(ValuePipe(2)).values }
        },
        Gen.const( { t: TypedPipe[Int] => t.debug }),
        Arbitrary.arbitrary[Int => Boolean].map { fn =>
          { t: TypedPipe[Int] => t.filter(fn) }
        },
        Gen.const( { t: TypedPipe[Int] => t.forceToDisk }),
        Gen.const( { t: TypedPipe[Int] => t.fork }),
        tpGen.map { p: TypedPipe[Int] =>
          { x: TypedPipe[Int] => x ++ p }
        },
        Gen.identifier.map { id =>
          { t: TypedPipe[Int] => t.addTrap(TypedText.tsv[Int](id)) }
        },
        Gen.identifier.map { id =>
          { t: TypedPipe[Int] => t.withDescription(id) }
        }
      )

    val one = for {
      n <- next1
      p <- tpGen
    } yield n(p)

    val next2: Gen[TypedPipe[(Int, Int)] => TypedPipe[Int]] =
      Gen.oneOf(
        Gen.const( { p: TypedPipe[(Int, Int)] => p.values }),
        Gen.const( { p: TypedPipe[(Int, Int)] => p.keys })
      )

    val two = for {
      n <- next2
      p <- keyed
    } yield n(p)

    Gen.frequency((3, one), (1, two))
  }

  lazy val keyed: Gen[TypedPipe[(Int, Int)]] =
    Gen.oneOf(
      for {
        single <- tpGen
        fn <- Arbitrary.arbitrary[Int => (Int, Int)]
      } yield single.map(fn),
      for {
        single <- tpGen
        fn <- Arbitrary.arbitrary[Int => List[(Int, Int)]]
      } yield single.flatMap(fn),
      for {
        fn <- Arbitrary.arbitrary[Int => Boolean]
        pair <- keyed
      } yield pair.filterKeys(fn),
      for {
        fn <- Arbitrary.arbitrary[Int => List[Int]]
        pair <- keyed
      } yield pair.flatMapValues(fn),
      for {
        fn <- Arbitrary.arbitrary[Int => Int]
        pair <- keyed
      } yield pair.mapValues(fn),
      for {
        pair <- Gen.lzy(keyed)
      } yield pair.sumByKey.toTypedPipe,
      for {
        pair <- Gen.lzy(keyed)
      } yield pair.sumByLocalKeys,
      for {
        pair <- Gen.lzy(keyed)
      } yield pair.group.mapGroup { (k, its) => its }.toTypedPipe,
      for {
        pair <- Gen.lzy(keyed)
      } yield pair.group.sorted.mapGroup { (k, its) => its }.toTypedPipe,
      for {
        pair <- Gen.lzy(keyed)
      } yield pair.group.sorted.withReducers(2).mapGroup { (k, its) => its }.toTypedPipe,
      for {
        p1 <- Gen.lzy(keyed)
        p2 <- Gen.lzy(keyed)
      } yield p1.hashJoin(p2).values,
      for {
        p1 <- Gen.lzy(keyed)
        p2 <- Gen.lzy(keyed)
      } yield p1.join(p2).values,
      for {
        p1 <- Gen.lzy(keyed)
        p2 <- Gen.lzy(keyed)
      } yield p1.join(p2).mapValues { case (a, b) => a * b }.toTypedPipe
    )

  val tpGen: Gen[TypedPipe[Int]] =
    Gen.lzy(Gen.frequency((1, srcGen), (1, mapped)))
}

class OptimizationRulesTest extends FunSuite {
  import OptimizationRules.toLiteral

  def invert[T](t: TypedPipe[T]) =
    assert(toLiteral(t).evaluate == t)

  test("randomly generated TypedPipe trees are invertible") {
    forAll(TypedPipeGen.tpGen) { (t: TypedPipe[Int]) =>
      invert(t)
    }
  }

  test("OptimizationRules.toLiteral is invertible on some specific instances") {

    invert(TypedPipe.from(TypedText.tsv[Int]("foo")))
    invert(TypedPipe.from(List(1, 2, 3)))
    invert(TypedPipe.from(List(1, 2, 3)).map(_ * 2))
    invert {
      TypedPipe.from(List(1, 2, 3)).map { i => (i, i) }.sumByKey.toTypedPipe
    }

    invert {
      val p = TypedPipe.from(List(1, 2, 3)).map { i => (i, i) }.sumByKey

      p.mapGroup { (k, its) => Iterator.single(its.sum * k) }
    }

    invert {
      val p = TypedPipe.from(List(1, 2, 3)).map { i => (i, i) }.sumByKey
      p.cross(TypedPipe.from(List("a", "b", "c")).sum)
    }

    invert {
      val p = TypedPipe.from(List(1, 2, 3)).map { i => (i, i) }.sumByKey
      p.cross(TypedPipe.from(List("a", "b", "c")))
    }

    invert {
      val p = TypedPipe.from(List(1, 2, 3)).map { i => (i, i) }.sumByKey
      p.forceToDisk
    }

    invert {
      val p = TypedPipe.from(List(1, 2, 3)).map { i => (i, i) }.sumByKey
      p.fork
    }

    invert {
      val p1 = TypedPipe.from(List(1, 2, 3)).map { i => (i, i) }
      val p2 = TypedPipe.from(TypedText.tsv[(Int, String)]("foo"))

      p1.join(p2).toTypedPipe
    }

    invert {
      val p1 = TypedPipe.from(List(1, 2, 3)).map { i => (i, i) }
      val p2 = TypedPipe.from(TypedText.tsv[(Int, String)]("foo"))

      p1.hashJoin(p2)
    }

    invert {
      val p1 = TypedPipe.from(List(1, 2, 3)).map { i => (i, i) }
      val p2 = TypedPipe.from(TypedText.tsv[(Int, String)]("foo"))

      p1.join(p2).filterKeys(_ % 2 == 0)
    }
  }
}
