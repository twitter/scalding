package com.twitter.scalding.typed

import cascading.flow.FlowDef
import cascading.tuple.Fields
import com.stripe.dagon.{ Dag, Rule }
import com.twitter.scalding.source.{ TypedText, NullSink }
import org.apache.hadoop.conf.Configuration
import com.twitter.scalding.{ Config, ExecutionContext, Local, Hdfs, FlowState, FlowStateMap, IterableSource }
import com.twitter.scalding.typed.cascading_backend.CascadingBackend
import org.scalatest.FunSuite
import org.scalatest.prop.PropertyChecks.forAll
import org.scalatest.prop.GeneratorDrivenPropertyChecks.PropertyCheckConfiguration
import org.scalacheck.{ Arbitrary, Gen }
import scala.util.{ Failure, Success, Try }
import com.twitter.scalding.quotation.Quoted

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

  import OptimizationRules._

  val allRules = List(
    AddExplicitForks,
    ComposeFlatMap,
    ComposeMap,
    ComposeFilter,
    ComposeWithOnComplete,
    ComposeMapFlatMap,
    ComposeFilterFlatMap,
    ComposeFilterMap,
    DescribeLater,
    RemoveDuplicateForceFork,
    IgnoreNoOpGroup,
    DeferMerge,
    FilterKeysEarly,
    EmptyIsOftenNoOp,
    EmptyIterableIsEmpty,
    ForceToDiskBeforeHashJoin,
    ApplyProjectionPushdown)

  def genRuleFrom(rs: List[Rule[TypedPipe]]): Gen[Rule[TypedPipe]] =
    for {
      c <- Gen.choose(1, rs.size)
      rs <- Gen.pick(c, rs)
    } yield rs.reduce(_.orElse(_))

  val genRule: Gen[Rule[TypedPipe]] = genRuleFrom(allRules)
}

/**
 * Used to test that we call phases
 */
class ThrowingOptimizer extends OptimizationPhases {
  def phases = sys.error("booom")
}

/**
 * Just convert everything to a constant
 *  so we can check that the optimization was applied
 */
class ConstantOptimizer extends OptimizationPhases {
  def phases = List(new Rule[TypedPipe] {
    def apply[T](on: Dag[TypedPipe]) = { t =>
      Some(TypedPipe.empty)
    }
  })
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

    // We don't want any further optimization on this job
    val conf = Config.empty.setOptimizationPhases(classOf[EmptyOptimizationPhases])
    assert(TypedPipeDiff.diff(t, optimized)
      .toIterableExecution
      .waitFor(conf, Local(true)).get.isEmpty)
  }

  def optimizationReducesSteps[T](init: TypedPipe[T], rule: Rule[TypedPipe]) = {
    val optimized = Dag.applyRule(init, toLiteral, rule)

    // How many steps would this be in Hadoop on Cascading
    def steps(p: TypedPipe[T]): Int = {
      val mode = Hdfs.default
      val fd = new FlowDef
      val pipe = CascadingBackend.toPipeUnoptimized(p, NullSink.sinkFields)(fd, mode, NullSink.setter)
      NullSink.writeFrom(pipe)(fd, mode)
      val ec = ExecutionContext.newContext(Config.defaultFrom(mode))(fd, mode)
      val flow = ec.buildFlow.get
      flow.getFlowSteps.size
    }

    assert(steps(init) >= steps(optimized))
  }

  test("all optimization rules don't change results") {
    import TypedPipeGen.{ genWithIterableSources, genRule }
    implicit val generatorDrivenConfig: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 100000)
    forAll(genWithIterableSources, genRule)(optimizationLaw[Int] _)
  }

  test("all optimization rules do not increase steps") {
    import TypedPipeGen.{ allRules, genWithIterableSources, genRuleFrom }
    implicit val generatorDrivenConfig: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 1000)

    val possiblyIncreasesSteps: Set[Rule[TypedPipe]] =
      Set(OptimizationRules.AddExplicitForks, // explicit forks can cause cascading to add steps instead of recomputing values
        OptimizationRules.ForceToDiskBeforeHashJoin // adding a forceToDisk can increase the number of steps
        )

    val gen = genRuleFrom(allRules.filterNot(possiblyIncreasesSteps))

    forAll(genWithIterableSources, gen)(optimizationReducesSteps[Int] _)
  }

  test("ThrowingOptimizer is triggered") {
    forAll(TypedPipeGen.genWithFakeSources) { t =>
      val conf = new Configuration()
      conf.set(Config.OptimizationPhases, classOf[ThrowingOptimizer].getName)
      implicit val mode = Hdfs(true, conf)
      implicit val fd = new FlowDef
      Try(CascadingBackend.toPipe(t, new Fields("value"))) match {
        case Failure(ex) => assert(ex.getMessage == "booom")
        case Success(res) => fail(s"expected failure, got $res")
      }
    }

    forAll(TypedPipeGen.genWithFakeSources) { t =>
      val ex = t.toIterableExecution

      val config = Config.empty.setOptimizationPhases(classOf[ThrowingOptimizer])
      ex.waitFor(config, Local(true)) match {
        case Failure(ex) => assert(ex.getMessage == "booom")
        case Success(res) => fail(s"expected failure, got $res")
      }
    }
  }

  test("ConstantOptimizer is triggered") {
    forAll(TypedPipeGen.genWithFakeSources) { t =>
      val conf = new Configuration()
      conf.set(Config.OptimizationPhases, classOf[ConstantOptimizer].getName)
      implicit val mode = Hdfs(true, conf)
      implicit val fd = new FlowDef
      Try(CascadingBackend.toPipe(t, new Fields("value"))) match {
        case Failure(ex) => fail(s"$ex")
        case Success(pipe) =>
          FlowStateMap.get(fd) match {
            case None => fail("expected a flow state")
            case Some(FlowState(m, _)) =>
              assert(m.size == 1)
              m.head._2 match {
                case it: IterableSource[_] =>
                  assert(it.iter == Nil)
                case _ =>
                  fail(s"$m")
              }
          }
      }
    }

    forAll(TypedPipeGen.genWithFakeSources) { t =>
      val ex = t.toIterableExecution

      val config = Config.empty.setOptimizationPhases(classOf[ConstantOptimizer])
      ex.waitFor(config, Local(true)) match {
        case Failure(ex) => fail(s"$ex")
        case Success(res) => assert(res.isEmpty)
      }
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
      eqCheck(tp.distinctBy(fn0)(ordInt, implicitly[Quoted]))
    }
  }
}
