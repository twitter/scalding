package com.twitter.scalding.typed

import com.twitter.algebird.Monoid
import com.twitter.scalding.{ Config, Execution, Local, TupleConverter, TupleGetter }
import com.twitter.scalding.source.{ TypedText, NullSink }
import com.twitter.scalding.typed.cascading_backend.CascadingBackend
import com.stripe.dagon.Dag
import org.scalatest.FunSuite
import org.scalatest.prop.PropertyChecks

class WritePartitionerTest extends FunSuite with PropertyChecks {
  def fakeSource[T](id: Int): TypedSource[T] =
    TypedText.tsv[String](s"source_$id").asInstanceOf[TypedSource[T]]

  case class WriteState(
    writes: List[WritePartitioner.PairK[TypedPipe, TypedSink, _]],
    materializations: List[WritePartitioner.PairK[TypedPipe, TypedSource, _]]) {

    def ++(that: WriteState): WriteState =
      WriteState(writes ::: that.writes,
        materializations ::: that.materializations)
  }

  object WriteState {
    def empty: WriteState = WriteState(List.empty, List.empty)
  }

  case class State[+T](writes: WriteState, value: T)
  object State {
    implicit val materializer: WritePartitioner.Materializer[State] =
      new WritePartitioner.Materializer[State] {
        def pure[A](a: A) = State(WriteState(List.empty, List.empty), a)
        def map[A, B](ma: State[A])(fn: A => B) =
          State(ma.writes, fn(ma.value))

        def zip[A, B](ma: State[A], mb: State[B]) =
          State(ma.writes ++ mb.writes, (ma.value, mb.value))

        def materialize[A](t: State[TypedPipe[A]]): State[TypedPipe[A]] = {
          val fakeReader = fakeSource[A](t.writes.materializations.size)
          val newMats: List[WritePartitioner.PairK[TypedPipe, TypedSource, _]] =
            (t.value, fakeReader) :: t.writes.materializations
          State(t.writes.copy(materializations = newMats), TypedPipe.from(fakeReader))
        }
        def write[A](tp: State[TypedPipe[A]], sink: TypedSink[A]): State[Unit] = {
          val newWrites: List[WritePartitioner.PairK[TypedPipe, TypedSink, _]] =
            (tp.value, sink) :: tp.writes.writes
          State(tp.writes.copy(writes = newWrites), ())
        }
        def sequence_[A](as: Seq[State[A]]): State[Unit] =
          // just merge them all together:
          State(as.foldLeft(WriteState.empty) { (old, n) =>
            n.writes ++ old
          }, ())
      }
  }

  test("When we break at forks we have at most 2 + hashJoin steps") {
    implicit val generatorDrivenConfig: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 100)

    def afterPartitioningEachStepIsSize1[T](init: TypedPipe[T]) = {
      val phases = CascadingBackend.defaultOptimizationRules(Config.empty)

      val writes = WritePartitioner.materialize[State](phases, List((init, NullSink))).writes

      writes.writes.foreach {
        case (tp, _) =>
          val (dag, _) = Dag(tp, OptimizationRules.toLiteral)
          val hcg = dag.allNodes.collect { case h: TypedPipe.HashCoGroup[_, _, _, _] => 1 }.sum
          // we can have at most 2 + hcg jobs
          assert(TypedPipeGen.steps(tp) <= 2 + hcg, s"optimized: ${tp.toString}")
      }
      writes.materializations.foreach {
        case (tp, _) =>
          val (dag, _) = Dag(tp, OptimizationRules.toLiteral)
          val hcg = dag.allNodes.collect { case h: TypedPipe.HashCoGroup[_, _, _, _] => 1 }.sum
          // we can have at most 1 + hcg jobs
          assert(TypedPipeGen.steps(tp) <= 2 + hcg, s"optimized: ${tp.toString}")
      }
    }

    forAll(TypedPipeGen.genWithFakeSources)(afterPartitioningEachStepIsSize1(_))
  }

  test("the total number of steps is not more than cascading") {
    def notMoreSteps[T](t: TypedPipe[T]) = {
      val phases = CascadingBackend.defaultOptimizationRules(Config.empty)

      val writes = WritePartitioner.materialize[State](phases, List((t, NullSink))).writes

      val writeSteps = writes.writes.map {
        case (tp, _) => TypedPipeGen.steps(tp)
      }.sum
      val matSteps = writes.materializations.map {
        case (tp, _) => TypedPipeGen.steps(tp)
      }.sum
      val (dag, id) = Dag(t, OptimizationRules.toLiteral)
      val optDag = dag.applySeq(phases)
      val optT = optDag.evaluate(id)
      assert(writeSteps + matSteps <= TypedPipeGen.steps(optT))
    }

    {
      import TypedPipe._

      val pipe = WithDescriptionTypedPipe(Mapped(ReduceStepPipe(ValueSortedReduce[Int, Int, Int](implicitly[Ordering[Int]],
        WithDescriptionTypedPipe(WithDescriptionTypedPipe(Mapped(WithDescriptionTypedPipe(MergedTypedPipe(
          WithDescriptionTypedPipe(Fork(WithDescriptionTypedPipe(TrappedPipe(SourcePipe(TypedText.tsv[Int]("oyg")),
            TypedText.tsv[Int]("a3QasphTfqhd1namjb"),
            TupleConverter.Single(implicitly[TupleGetter[Int]])), List(("org.scalacheck.Gen$R $class.map(Gen.scala:237)", true)))),
            List(("org.scalacheck.Gen$R$class.map(Gen.scala:237)", true))),
          IterablePipe(List(-930762680, -1495455462, -1, -903011942, -2147483648, 1539778843, -2147483648))),
          List(("org.scalacheck.Gen$R$class.map(Gen.scala:237)", true))), null /*<function1>*/ ),
          List(("org.scalacheck.Gen$R$class.map(Gen.scala:237)", true))),
          List(("org.scalacheck.Gen$R$class.map(Gen.scala:237)", true))),
        implicitly[Ordering[Int]], null /*<function2>*/ , Some(2), List())),
        null /*<function1>*/ ), List(("org.scalacheck.Gen$R$class.map(Gen.scala:237)", true)))

      notMoreSteps(pipe)
    }

    {
      import TypedPipe._

      val pipe = WithDescriptionTypedPipe(ForceToDisk(WithDescriptionTypedPipe(Mapped(
        ReduceStepPipe(ValueSortedReduce[Int, Int, Int](implicitly[Ordering[Int]],
          WithDescriptionTypedPipe(WithDescriptionTypedPipe(
            Mapped(WithDescriptionTypedPipe(MergedTypedPipe(WithDescriptionTypedPipe(
              Mapped(WithDescriptionTypedPipe(CrossValue(
                SourcePipe(TypedText.tsv[Int]("yumwd")), LiteralValue(2)),
                List(("org.scalacheck.Gen$R$class.map(Gen.scala:237)", true))), null /*<function1>*/ ),
              List(("org.scalacheck.Gen$R$class.map(Gen.scala:237)", true))),
              WithDescriptionTypedPipe(Mapped(WithDescriptionTypedPipe(FilterKeys(
                WithDescriptionTypedPipe(SumByLocalKeys(
                  WithDescriptionTypedPipe(FlatMapped(
                    IterablePipe(List(943704575)), null /*<function1>*/ ),
                    List(("org.scalacheck.Gen$R$class.map(Gen.scala:237)", true))),
                  implicitly[Monoid[Int]]),
                  List(("org.scalacheck.Gen$R$class.map(Gen.scala:237)", true))), null /*<function1>*/ ),
                List(("org.scalacheck.Gen$R$class.map(Gen.scala:237)", true))), null /*<function1>*/ ),
                List(("org.scalacheck.Gen$R$class.map(Gen.scala:237)", true)))),
              List(("org.scalacheck.Gen$R$class.map(Gen.scala:237)", true))), null /*<function1>*/ ),
            List(("org.scalacheck.Gen$R$class.map(Gen.scala:237)", true))),
            List(("org.scalacheck.Gen$R$class.map(Gen.scala:237)", true))),
          implicitly[Ordering[Int]], null /*<function2>*/ , None, List())),
        null /*<function1>*/ ),
        List(("org.scalacheck.Gen$R$class.map(Gen.scala:237)", true)))),
        List(("org.scalacheck.Gen$R$class.map(Gen.scala:237)", true)))

      notMoreSteps(pipe)
    }

    {
      import TypedPipe._

      val pipe = WithDescriptionTypedPipe(
        Fork(WithDescriptionTypedPipe(Mapped(WithDescriptionTypedPipe(CrossValue(
          WithDescriptionTypedPipe(TrappedPipe(WithDescriptionTypedPipe(ForceToDisk(WithDescriptionTypedPipe(
            Mapped(ReduceStepPipe(ValueSortedReduce[Int, Int, Int](implicitly[Ordering[Int]],
              WithDescriptionTypedPipe(WithDescriptionTypedPipe(FilterKeys(WithDescriptionTypedPipe(FlatMapValues(
                WithDescriptionTypedPipe(Mapped(IterablePipe(List(1533743286, 0, -1, 0, 1637692751)),
                  null /*<function1>*/ ),
                  List(("org.scalacheck.Gen$R$class.map(Gen.scala:237)", true))), null /*<function1>*/ ),
                List(("org.scalacheck.Gen$R$class.map(Gen.scala:237)", true))), null /*<function1>*/ ),
                List(("org.scalacheck.Gen$R$class.map(Gen.scala:237)", true))),
                List(("org.scalacheck.Gen$R$class.map(Gen.scala:237)", true))),
              implicitly[Ordering[Int]], null /*<function2>*/ , Some(2), List())),
              null /*<function1>*/ ),
            List(("org.scalacheck.Gen$R$class.map(Gen.scala:237)", true)))),
            List(("org.scalacheck.Gen$R$class.map(Gen.scala:237)", true))),
            TypedText.tsv[Int]("mndlSTwuEmwqhJk7ac"),
            TupleConverter.Single(implicitly[TupleGetter[Int]])),
            List(("org.scalacheck.Gen$R$class.map(Gen.scala:237)", true))),
          LiteralValue(2)),
          List(("org.scalacheck.Gen$R$class.map(Gen.scala:237)", true))),
          null /*<function1>*/ ),
          List(("org.scalacheck.Gen$R$class.map(Gen.scala:237)", true)))),
        List(("org.scalacheck.Gen$R$class.map(Gen.scala:237)", true)))

      notMoreSteps(pipe)
    }
    implicit val generatorDrivenConfig: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 100)
    forAll(TypedPipeGen.genWithFakeSources)(notMoreSteps(_))
  }

  test("breaking things up does not change the results") {
    implicit val generatorDrivenConfig: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 100)

    def partitioningDoesNotChange[T: Ordering](init: TypedPipe[T]) = {
      val phases = CascadingBackend.defaultOptimizationRules(Config.empty)

      // We don't want any further optimization on this job
      val ex: Execution[TypedPipe[T]] = WritePartitioner.partitionSingle(phases, init)
      assert(ex.flatMap(TypedPipeDiff.diff[T](init, _).toIterableExecution)
        .waitFor(Config.empty, Local(true)).get.isEmpty)
    }

    forAll(TypedPipeGen.genWithIterableSources)(partitioningDoesNotChange(_))
  }
}
