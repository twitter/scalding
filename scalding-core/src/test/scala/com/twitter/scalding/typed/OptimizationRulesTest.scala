package com.twitter.scalding.typed

import cascading.flow.FlowDef
import cascading.tuple.Fields
import com.stripe.dagon.{ Dag, Rule }
import com.twitter.algebird.Monoid
import com.twitter.scalding.source.{ TypedText, NullSink }
import org.apache.hadoop.conf.Configuration
import com.twitter.scalding.{ Config, ExecutionContext, Local, Hdfs, FlowState, FlowStateMap, IterableSource }
import com.twitter.scalding.typed.cascading_backend.CascadingBackend
import org.scalatest.FunSuite
import org.scalatest.prop.PropertyChecks
import org.scalacheck.{ Arbitrary, Gen }
import scala.util.{ Failure, Success, Try }

object TypedPipeGen {
  val srcGen: Gen[TypedPipe[Int]] = {
    val g1 = Gen.listOf(Arbitrary.arbitrary[Int]).map(TypedPipe.from(_))
    val src = Gen.identifier.map { f => TypedPipe.from(TypedText.tsv[Int](f)) }
    Gen.oneOf(g1, src, Gen.const(TypedPipe.empty))
  }

  def mapped(srcGen: Gen[TypedPipe[Int]]): Gen[TypedPipe[Int]] = {
    val commonFreq = 10
    val next1: Gen[TypedPipe[Int] => TypedPipe[Int]] =
      Gen.frequency(
        (1, tpGen(srcGen).map { p: TypedPipe[Int] =>
          { x: TypedPipe[Int] => x.cross(p).keys }
        }),
        (2, tpGen(srcGen).map { p: TypedPipe[Int] =>
          { x: TypedPipe[Int] => x.cross(ValuePipe(2)).values }
        }),
        //Gen.const({ t: TypedPipe[Int] => t.debug }), debug spews a lot to the terminal
        (commonFreq, Arbitrary.arbitrary[Int => Boolean].map { fn =>
          { t: TypedPipe[Int] => t.filter(fn) }
        }),
        (commonFreq, Arbitrary.arbitrary[Int => Int].map { fn =>
          { t: TypedPipe[Int] => t.map(fn) }
        }),
        (commonFreq, Arbitrary.arbitrary[Int => List[Int]].map { fn =>
          { t: TypedPipe[Int] => t.flatMap(fn.andThen(_.take(4))) } // the take is to not get too big
        }),
        (2, Gen.const({ t: TypedPipe[Int] => t.forceToDisk })),
        (2, Gen.const({ t: TypedPipe[Int] => t.fork })),
        (5, tpGen(srcGen).map { p: TypedPipe[Int] =>
          { x: TypedPipe[Int] => x ++ p }
        }),
        (1, Gen.identifier.map { id =>
          { t: TypedPipe[Int] => t.addTrap(TypedText.tsv[Int](id)) }
        }),
        (1, Gen.identifier.map { id =>
          { t: TypedPipe[Int] => t.withDescription(id) }
        }))

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
      } yield single.flatMap(fn.andThen(_.take(4))) // take to not get too big
      )

    val two = Gen.oneOf(
      for {
        fn <- Arbitrary.arbitrary[Int => Boolean]
        pair <- keyRec
      } yield pair.filterKeys(fn),
      for {
        fn <- Arbitrary.arbitrary[Int => List[Int]]
        pair <- keyRec
      } yield pair.flatMapValues(fn.andThen(_.take(4))), // take to not get too big
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
      } yield p1.hashJoin(p2).mapValues { case (a, b) => 31 * a + b },
      for {
        p1 <- keyRec
        p2 <- keyRec
      } yield p1.join(p2).values,
      for {
        p1 <- keyRec
        p2 <- keyRec
      } yield p1.join(p2).mapValues { case (a, b) => a + 31 * b }.toTypedPipe)

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
    Gen.choose(0, 16) // don't make giant lists which take too long to evaluate
      .flatMap { sz =>
        tpGen(Gen.listOfN(sz, Arbitrary.arbitrary[Int]).map(TypedPipe.from(_)))
      }

  val genKeyedWithFake: Gen[TypedPipe[(Int, Int)]] =
    keyed(srcGen)

  import OptimizationRules._

  val allRules = List(
    AddExplicitForks,
    ComposeDescriptions,
    ComposeFlatMap,
    ComposeMap,
    ComposeFilter,
    ComposeWithOnComplete,
    ComposeMapFlatMap,
    ComposeFilterFlatMap,
    ComposeFilterMap,
    ComposeReduceSteps,
    DescribeLater,
    DiamondToFlatMap,
    RemoveDuplicateForceFork,
    IgnoreNoOpGroup,
    DeferMerge,
    FilterKeysEarly,
    FilterLocally,
    //EmptyIsOftenNoOp, this causes confluence problems when combined with other rules randomly.
    //Have to be careful about the order it is applied
    EmptyIterableIsEmpty,
    HashToShuffleCoGroup,
    ForceToDiskBeforeHashJoin,
    MapValuesInReducers)

  def genRuleFrom(rs: List[Rule[TypedPipe]]): Gen[Rule[TypedPipe]] =
    for {
      c <- Gen.choose(1, rs.size)
      rs <- Gen.pick(c, rs)
    } yield rs.reduce(_.orElse(_))

  val genRule: Gen[Rule[TypedPipe]] = genRuleFrom(allRules)

  // How many steps would this be in Hadoop on Cascading
  def steps[A](p: TypedPipe[A]): Int = {
    val mode = Hdfs.default
    val fd = new FlowDef
    val pipe = CascadingBackend.toPipeUnoptimized(p, NullSink.sinkFields)(fd, mode, NullSink.setter)
    NullSink.writeFrom(pipe)(fd, mode)
    val ec = ExecutionContext.newContext(Config.defaultFrom(mode))(fd, mode)
    val flow = ec.buildFlow.get.get
    flow.getFlowSteps.size
  }

  // How many steps would this be in Hadoop on Cascading
  def optimizedSteps[A](rs: List[Rule[TypedPipe]], maxSteps: Int)(pipe: TypedPipe[A]) = {
    val (dag, id) = Dag(pipe, OptimizationRules.toLiteral)
    val optDag = dag.applySeq(rs)
    val optPipe = optDag.evaluate(id)
    val s = steps(optPipe)
    assert(s <= maxSteps, s"$s > $maxSteps. optimized: $optPipe")
  }
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

// we need to extend PropertyChecks, it seems, to control the number of successful runs
// for optimization rules, we want to do many tests
class OptimizationRulesTest extends FunSuite with PropertyChecks {
  import OptimizationRules.toLiteral
  import TypedPipeGen.optimizedSteps

  def invert[T](t: TypedPipe[T]) =
    assert(toLiteral(t).evaluate == t)

  test("randomly generated TypedPipe trees are invertible") {
    forAll(TypedPipeGen.genWithFakeSources) { (t: TypedPipe[Int]) =>
      invert(t)
    }
  }

  test("optimization rules are reproducible") {
    import TypedPipeGen.{ genWithFakeSources, genRule }

    implicit val generatorDrivenConfig: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 500)
    forAll(genWithFakeSources, genRule) { (t, rule) =>
      val optimized = Dag.applyRule(t, toLiteral, rule)
      val optimized2 = Dag.applyRule(t, toLiteral, rule)
      assert(optimized == optimized2)
    }
  }

  test("standard rules are reproducible") {
    import TypedPipeGen.genWithFakeSources

    implicit val generatorDrivenConfig: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 500)
    forAll(genWithFakeSources) { t =>
      val (dag1, id1) = Dag(t, toLiteral)
      val opt1 = dag1.applySeq(OptimizationRules.standardMapReduceRules)
      val t1 = opt1.evaluate(id1)

      val (dag2, id2) = Dag(t, toLiteral)
      val opt2 = dag2.applySeq(OptimizationRules.standardMapReduceRules)
      val t2 = opt2.evaluate(id2)
      assert(t1 == t2)
    }
  }

  def optimizationLaw[T: Ordering](t: TypedPipe[T], rule: Rule[TypedPipe]) = {
    val optimized = Dag.applyRule(t, toLiteral, rule)

    // We don't want any further optimization on this job
    val conf = Config.empty.setOptimizationPhases(classOf[EmptyOptimizationPhases])
    assert(TypedPipeDiff.diff(t, optimized)
      .toIterableExecution
      .waitFor(conf, Local(true)).get.isEmpty)
  }

  def optimizationReducesSteps[T](init: TypedPipe[T], rule: Rule[TypedPipe]) = {
    val optimized = Dag.applyRule(init, toLiteral, rule)

    assert(TypedPipeGen.steps(init) >= TypedPipeGen.steps(optimized))
  }

  test("all optimization rules don't change results") {
    import TypedPipeGen.{ genWithIterableSources, genRule }
    implicit val generatorDrivenConfig: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 50)
    forAll(genWithIterableSources, genRule)(optimizationLaw[Int] _)
  }

  test("some past failures of the optimizationLaw") {
    import TypedPipe._

    val arg01 = (TypedPipe.empty.withDescription("foo") ++ TypedPipe.empty.withDescription("bar")).addTrap(TypedText.tsv[Int]("foo"))
    optimizationLaw(arg01, Rule.empty)
  }

  test("all optimization rules do not increase steps") {
    import TypedPipeGen.{ allRules, genWithIterableSources, genRuleFrom }
    implicit val generatorDrivenConfig: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 200)

    val possiblyIncreasesSteps: Set[Rule[TypedPipe]] =
      Set(OptimizationRules.AddExplicitForks, // explicit forks can cause cascading to add steps instead of recomputing values
        OptimizationRules.ForceToDiskBeforeHashJoin, // adding a forceToDisk can increase the number of steps
        OptimizationRules.HashToShuffleCoGroup // obviously changing a hashjoin to a cogroup can increase steps
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
            case Some(FlowState(m, _, _)) =>
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
      eqCheck(tp.distinctBy(fn0)(ordInt))
    }
  }

  @annotation.tailrec
  final def fib[A](t0: A, t1: A, n: Int)(fn: (A, A) => A): A =
    if (n <= 0) t0
    else if (n == 1) t1
    else {
      val t2 = fn(t0, t1)
      fib(t1, t2, n - 1)(fn)
    }

  def isFasterThan[A](millis: Int)(a: => A) = {
    val start = System.nanoTime()
    val res = a
    val end = System.nanoTime()
    assert((end - start) / (1000L * 1000L) < millis)
  }

  test("Dagon relies on fast hashCodes and fast equality. Test some example ones to make sure they are not exponential") {

    def testFib(fn: (TypedPipe[Int], TypedPipe[Int]) => TypedPipe[Int]) =
      isFasterThan(1000){
        fib(TypedPipe.from(List(0)), TypedPipe.from(List(1, 2)), 45)(fn).hashCode
      }

    // Test the ways we can combine pipes
    testFib(_ ++ _)
    testFib(_.cross(_).map { case (a, b) => a * b })
    testFib { (left, right) =>
      left.asKeys.join(right.asKeys).keys
    }

    assert(TypedPipe.empty == TypedPipe.empty)
    // now test equality on a fib merge
    // without linear time equality, this fails when fib count is 35, at 50
    // it would take a huge amount of time
    isFasterThan(1000) {
      assert(fib(TypedPipe.from(List(0)), TypedPipe.from(List(1)), 50)(_ ++ _) ==
        fib(TypedPipe.from(List(0)), TypedPipe.from(List(1)), 50)(_ ++ _))
    }
  }

  test("write should be fast for large graphs") {
    val fd = new FlowDef
    isFasterThan(1000) {
      // for non-linear complexity this will fail
      fib(TypedPipe.from(List(0)), TypedPipe.from(List(1, 2)), 45) { (a, b) =>
        // write should be fast too
        a.write(NullSink)(fd, Local(true)) ++ b
      }
    }
  }

  test("joins are merged") {
    def kv(s: String) =
      TypedPipe.from(TypedText.tsv[(Int, Int)](s))

    optimizedSteps(OptimizationRules.standardMapReduceRules, 1) {
      kv("a").join(kv("b")).join(kv("c"))
    }
  }

  test("needless toTypedPipe is removed") {

    def kv(s: String) =
      TypedPipe.from(TypedText.tsv[(Int, Int)](s))

    optimizedSteps(OptimizationRules.standardMapReduceRules ::: List(OptimizationRules.ComposeReduceSteps), 1) {
      kv("a").sumByKey.toTypedPipe.group.max
    }

    optimizedSteps(OptimizationRules.standardMapReduceRules ::: List(OptimizationRules.ComposeReduceSteps), 1) {
      kv("a").join(kv("b")).toTypedPipe.group.max
    }

    optimizedSteps(OptimizationRules.standardMapReduceRules ::: List(OptimizationRules.ComposeReduceSteps), 1) {
      kv("a").sumByKey.toTypedPipe.join(kv("b")).toTypedPipe.group.max
    }

    optimizedSteps(OptimizationRules.standardMapReduceRules ::: List(OptimizationRules.ComposeReduceSteps), 1) {
      kv("a").join(kv("b").sumByKey.toTypedPipe).toTypedPipe.group.max
    }

    optimizedSteps(OptimizationRules.standardMapReduceRules ::: List(OptimizationRules.ComposeReduceSteps), 1) {
      kv("a").join(kv("b").sumByKey.toTypedPipe.mapValues(_ * 2)).toTypedPipe.group.max
    }

    optimizedSteps(OptimizationRules.standardMapReduceRules ::: List(OptimizationRules.ComposeReduceSteps), 1) {
      kv("a").join(kv("b").sumByKey.toTypedPipe.flatMapValues(0 to _)).toTypedPipe.group.max
    }

    optimizedSteps(OptimizationRules.standardMapReduceRules ::: List(OptimizationRules.ComposeReduceSteps), 1) {
      kv("a").join(kv("b").sumByKey.toTypedPipe.filterKeys(_ > 2)).toTypedPipe.group.max
    }
  }

  test("merge before reduce is one step") {
    def kv(s: String) =
      TypedPipe.from(TypedText.tsv[(Int, Int)](s))

    optimizedSteps(OptimizationRules.standardMapReduceRules, 1) {
      (kv("a") ++ kv("b")).sumByKey
    }

    optimizedSteps(OptimizationRules.standardMapReduceRules, 1) {
      (kv("a") ++ kv("b")).join(kv("c"))
    }
    optimizedSteps(OptimizationRules.standardMapReduceRules, 1) {
      kv("a").join(kv("b") ++ kv("c"))
    }

    optimizedSteps(OptimizationRules.standardMapReduceRules, 1) {
      val input = kv("a")
      val pipe1 = input.mapValues(_ * 2)
      val pipe2 = input.mapValues(_ * 3)
      (pipe1 ++ pipe2).sumByKey
    }

    optimizedSteps(OptimizationRules.standardMapReduceRules, 1) {
      val input = kv("a")
      val pipe1 = input.mapValues(_ * 2)
      val pipe2 = input.mapValues(_ * 3)
      (pipe1 ++ pipe2).mapValues(_ + 42).sumByKey
    }
  }

  test("merge after identical reduce reduce is one step") {
    def kv(s: String) =
      TypedPipe.from(TypedText.tsv[(Int, Int)](s))

    // TODO: currently fails, this is now 3 steps.
    // optimizedSteps(OptimizationRules.standardMapReduceRules, 1) {
    //   (kv("a").join(kv("b")) ++ kv("a").join(kv("c")))
    // }

    // TODO: currently fails, this is now 3 steps.
    // optimizedSteps(OptimizationRules.standardMapReduceRules, 1) {
    //   (kv("a").join(kv("b")) ++ kv("c").join(kv("b")))
    // }

    // TODO: currently fails, this is now 3 steps.
    // optimizedSteps(OptimizationRules.standardMapReduceRules, 1) {
    //   (kv("a").sumByKey.mapValues(_ * 10) ++ kv("a").sumByKey)
    // }

  }

  test("merging pipes does not make them unplannable") {
    val pipe1 = (TypedPipe.from(0 to 1000).map { x => (x, x) } ++
      (TypedPipe.from(0 to 2000).groupBy(_ % 17).sum.toTypedPipe))

    val pipe2 = (TypedPipe.from(0 to 1000) ++
      TypedPipe.from(0 to 2000).filter(_ % 17 == 0))

    val pipe3 = (TypedPipe.from(TypedText.tsv[Int]("src1")).map { x => (x, x) } ++
      (TypedPipe.from(TypedText.tsv[Int]("src2")).groupBy(_ % 17).sum.toTypedPipe))

    val pipe4 = (TypedPipe.from(TypedText.tsv[Int]("src1")) ++
      TypedPipe.from(TypedText.tsv[Int]("src2")).filter(_ % 17 == 0))

    optimizedSteps(OptimizationRules.standardMapReduceRules, 2)(pipe1)
    optimizedSteps(OptimizationRules.standardMapReduceRules, 1)(pipe2)
    optimizedSteps(OptimizationRules.standardMapReduceRules, 2)(pipe3)
    optimizedSteps(OptimizationRules.standardMapReduceRules, 1)(pipe4)
  }

  test("we can plan an enormous list of combined typedPipes") {
    // set this to 10,000 and use the default Monoid.plus
    // and this fails fast, but still planning a giant graph
    // is quadratic to apply the optimizations, so it takes
    // a long time, but does not stack overflow.
    val pipes = (0 to 1000).map(i => TypedPipe.from(List(i)))
    val pipe = Monoid.sum(pipes)

    optimizedSteps(OptimizationRules.standardMapReduceRules, 1)(pipe)
  }
}
