package com.twitter.scalding.typed

import com.stripe.dagon.{ Dag, Rule }
import com.twitter.scalding.source.TypedText
import com.twitter.scalding.{ Config, Local }
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

  def mapped(srcGen: Gen[TypedPipe[Int]]): Gen[TypedPipe[Int]] = {
    val mappedRec = Gen.lzy(mapped(srcGen))
    val next1: Gen[TypedPipe[Int] => TypedPipe[Int]] =
      Gen.oneOf(
        tpGen(srcGen).map { p: TypedPipe[Int] =>
          { x: TypedPipe[Int] => x.cross(p).keys }
        },
        tpGen(srcGen).map { p: TypedPipe[Int] =>
          { x: TypedPipe[Int] => x.cross(ValuePipe(2)).values }
        },
        Gen.const({ t: TypedPipe[Int] => t.debug }),
        Arbitrary.arbitrary[Int => Boolean].map { fn =>
          { t: TypedPipe[Int] => t.filter(fn) }
        },
        Gen.const({ t: TypedPipe[Int] => t.forceToDisk }),
        Gen.const({ t: TypedPipe[Int] => t.fork }),
        tpGen(srcGen).map { p: TypedPipe[Int] =>
          { x: TypedPipe[Int] => x ++ p }
        },
        Gen.identifier.map { id =>
          { t: TypedPipe[Int] => t.addTrap(TypedText.tsv[Int](id)) }
        },
        Gen.identifier.map { id =>
          { t: TypedPipe[Int] => t.withDescription(id) }
        })

    val one = for {
      n <- next1
      p <- tpGen(srcGen)
    } yield n(p)

    val next2: Gen[TypedPipe[(Int, Int)] => TypedPipe[Int]] =
      Gen.oneOf(
        Gen.const({ p: TypedPipe[(Int, Int)] => p.values }),
        Gen.const({ p: TypedPipe[(Int, Int)] => p.keys }))

    val two = for {
      n <- next2
      p <- keyed(srcGen)
    } yield n(p)

    Gen.frequency((4, one), (1, two))
  }

  def keyed(srcGen: Gen[TypedPipe[Int]]): Gen[TypedPipe[(Int, Int)]] = {
    val keyRec = Gen.lzy(keyed(srcGen))
    val one = Gen.oneOf(
      for {
        single <- tpGen(srcGen)
        fn <- Arbitrary.arbitrary[Int => (Int, Int)]
      } yield single.map(fn),
      for {
        single <- tpGen(srcGen)
        fn <- Arbitrary.arbitrary[Int => List[(Int, Int)]]
      } yield single.flatMap(fn))

    val two = Gen.oneOf(
      for {
        fn <- Arbitrary.arbitrary[Int => Boolean]
        pair <- keyRec
      } yield pair.filterKeys(fn),
      for {
        fn <- Arbitrary.arbitrary[Int => List[Int]]
        pair <- keyRec
      } yield pair.flatMapValues(fn),
      for {
        fn <- Arbitrary.arbitrary[Int => Int]
        pair <- keyRec
      } yield pair.mapValues(fn),
      for {
        pair <- keyRec
      } yield pair.sumByKey.toTypedPipe,
      for {
        pair <- keyRec
      } yield pair.sumByLocalKeys,
      for {
        pair <- keyRec
      } yield pair.group.mapGroup { (k, its) => its }.toTypedPipe,
      for {
        pair <- keyRec
      } yield pair.group.sorted.mapGroup { (k, its) => its }.toTypedPipe,
      for {
        pair <- keyRec
      } yield pair.group.sorted.withReducers(2).mapGroup { (k, its) => its }.toTypedPipe,
      for {
        p1 <- keyRec
        p2 <- keyRec
      } yield p1.hashJoin(p2).values,
      for {
        p1 <- keyRec
        p2 <- keyRec
      } yield p1.join(p2).values,
      for {
        p1 <- keyRec
        p2 <- keyRec
      } yield p1.join(p2).mapValues { case (a, b) => a * b }.toTypedPipe)

    // bias to consuming Int, since the we can stack overflow with the (Int, Int)
    // cases
    Gen.frequency((2, one), (1, two))
  }

  def tpGen(srcGen: Gen[TypedPipe[Int]]): Gen[TypedPipe[Int]] =
    Gen.lzy(Gen.frequency((1, srcGen), (1, mapped(srcGen))))

  /**
   * This generates a TypedPipe that can't neccesarily
   * be run because it has fake sources
   */
  val genWithFakeSources: Gen[TypedPipe[Int]] = tpGen(srcGen)

  /**
   * This can always be run because all the sources are
   * Iterable sources
   */
  val genWithIterableSources: Gen[TypedPipe[Int]] =
    tpGen(Gen.listOf(Arbitrary.arbitrary[Int]).map(TypedPipe.from(_)))

  val genKeyedWithFake: Gen[TypedPipe[(Int, Int)]] =
    keyed(srcGen)
}

class OptimizationRulesTest extends FunSuite {
  import OptimizationRules.toLiteral

  def invert[T](t: TypedPipe[T]) =
    assert(toLiteral(t).evaluate == t)

  test("randomly generated TypedPipe trees are invertible") {
    forAll(TypedPipeGen.genWithFakeSources) { (t: TypedPipe[Int]) =>
      invert(t)
    }
  }

  def optimizationLaw[T: Ordering](t: TypedPipe[T], rule: Rule[TypedPipe]) = {
    val optimized = Dag.applyRule(t, toLiteral, rule)
    val optimized2 = Dag.applyRule(t, toLiteral, rule)

    // Optimization pure is function (wrt to universal equality)
    assert(optimized == optimized2)

    // optimizatized pipes are identical to initial state
    assert(TypedPipeDiff.diff(t, optimized)
      .toIterableExecution
      .waitFor(Config.empty, Local(true)).get.isEmpty)
  }

  test("all optimization rules don't change results") {
    import OptimizationRules._

    val allRules = List(ComposeFlatMap,
      ComposeMap,
      ComposeFilter,
      ComposeWithOnComplete,
      RemoveDuplicateForceFork,
      IgnoreNoOpGroup,
      DeferMerge,
      FilterKeysEarly,
      EmptyIsOftenNoOp,
      EmptyIterableIsEmpty)

    val genRule = for {
      c <- Gen.choose(1, allRules.size)
      rs <- Gen.pick(c, allRules)
    } yield rs.reduce(_.orElse(_))

    forAll(TypedPipeGen.genWithIterableSources, genRule)(optimizationLaw[Int] _)
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

  test("all transforms preserve equality") {

    forAll(TypedPipeGen.genWithFakeSources, TypedPipeGen.genKeyedWithFake) { (tp, keyed) =>
      val fn0 = { i: Int => i * 2 }
      val filterFn = { i: Int => i % 2 == 0 }
      val fn1 = { i: Int => (0 to i) }

      def eqCheck[T](t: => T) = {
        assert(t == t)
      }

      eqCheck(tp.map(fn0))
      eqCheck(tp.filter(filterFn))
      eqCheck(tp.flatMap(fn1))

      eqCheck(keyed.mapValues(fn0))
      eqCheck(keyed.flatMapValues(fn1))
      eqCheck(keyed.filterKeys(filterFn))

      eqCheck(tp.groupAll)
      eqCheck(tp.groupBy(fn0))
      eqCheck(tp.asKeys)
      eqCheck(tp.either(keyed))
      eqCheck(keyed.eitherValues(keyed.mapValues(fn0)))
      eqCheck(tp.map(fn1).flatten)
      eqCheck(keyed.swap)
      eqCheck(keyed.keys)
      eqCheck(keyed.values)

      val valueFn: (Int, Option[Int]) => String = { (a, b) => a.toString + b.toString }
      val valueFn2: (Int, Option[Int]) => List[Int] = { (a, b) => a :: (b.toList) }
      val valueFn3: (Int, Option[Int]) => Boolean = { (a, b) => true }

      eqCheck(tp.mapWithValue(LiteralValue(1))(valueFn))
      eqCheck(tp.flatMapWithValue(LiteralValue(1))(valueFn2))
      eqCheck(tp.filterWithValue(LiteralValue(1))(valueFn3))

      eqCheck(tp.hashLookup(keyed))
      eqCheck(tp.groupRandomly(100))
      val ordInt = implicitly[Ordering[Int]]
      eqCheck(tp.distinctBy(fn0)(ordInt))
    }
  }
}
