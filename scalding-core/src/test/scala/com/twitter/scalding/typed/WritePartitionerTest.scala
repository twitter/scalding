package com.twitter.scalding.typed

import com.twitter.scalding.{ Config, Execution, Local }
import com.twitter.scalding.source.{ TypedText, NullSink }
import com.twitter.scalding.typed.cascading_backend.CascadingBackend
import com.twitter.scalding.typed.functions.EqTypes
import com.stripe.dagon.Dag
import org.scalatest.FunSuite
import org.scalatest.prop.GeneratorDrivenPropertyChecks.PropertyCheckConfiguration
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

  test("When we break at forks we have at most 1 + hashJoin steps") {
    implicit val generatorDrivenConfig: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 100)

    def afterPartitioningEachStepIsSize1[T](init: TypedPipe[T]) = {
      val phases = CascadingBackend.defaultOptimizationRules(Config.empty)

      val writes = WritePartitioner.materialize[State](phases, List((init, NullSink))).writes

      writes.writes.foreach {
        case (tp, _) =>
          val (dag, _) = Dag(tp, OptimizationRules.toLiteral)
          val hcg = dag.allNodes.collect { case h: TypedPipe.HashCoGroup[_, _, _, _] => 1 }.sum
          // we can have at most 1 + hcg jobs
          assert(TypedPipeGen.steps(tp) <= 1 + hcg, s"optimized: ${tp.toString}")
      }
      writes.materializations.foreach {
        case (tp, _) =>
          val (dag, _) = Dag(tp, OptimizationRules.toLiteral)
          val hcg = dag.allNodes.collect { case h: TypedPipe.HashCoGroup[_, _, _, _] => 1 }.sum
          // we can have at most 1 + hcg jobs
          assert(TypedPipeGen.steps(tp) <= 1 + hcg, s"optimized: ${tp.toString}")
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
      assert(writeSteps + matSteps <= TypedPipeGen.steps(optT) + 1)
    }

    implicit val generatorDrivenConfig: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 100)
    forAll(TypedPipeGen.genWithFakeSources)(notMoreSteps(_))
  }

  test("breaking things up does not change the results") {
    implicit val generatorDrivenConfig: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 100)

    def partitioningDoesNotChange[T: Ordering](init: TypedPipe[T]) = {
      val phases = CascadingBackend.defaultOptimizationRules(Config.empty)

      type Const[A] = EqTypes[A, T]
      implicit val matEx: WritePartitioner.Materializer[Execution] =
        WritePartitioner.Materializer.executionMaterializer

      val writes = WritePartitioner.materialize1[Execution, Const](phases, List((init, EqTypes.reflexive[T])))(matEx)
      assert(writes.size == 1)

      def fix[F[_], A](t: WritePartitioner.PairK[F, Const, A]): F[T] =
        t._2.subst[F](t._1)

      // We don't want any further optimization on this job
      val ex: Execution[TypedPipe[T]] = fix(writes.head)
      assert(ex.flatMap(TypedPipeDiff.diff[T](init, _).toIterableExecution)
        .waitFor(Config.empty, Local(true)).get.isEmpty)
    }

    forAll(TypedPipeGen.genWithIterableSources)(partitioningDoesNotChange(_))
  }
}
