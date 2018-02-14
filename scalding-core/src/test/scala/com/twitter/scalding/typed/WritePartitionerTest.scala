package com.twitter.scalding.typed

import com.twitter.scalding.Config
import com.twitter.scalding.source.{ TypedText, NullSink }
import com.twitter.scalding.typed.cascading_backend.CascadingBackend
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
}
