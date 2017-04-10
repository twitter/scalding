package com.twitter.scalding.bdd

import com.twitter.scalding.TypedPipe
import com.twitter.scalding.Dsl

trait TypedPipeOperationsConversions {
  import Dsl._
  import com.twitter.scalding.typed.TDsl._

  trait TypedPipeOperation[TypeOut] {
    def assertPipeSize(pipes: List[TypedPipe[_]], expectedSize: Int) =
      require(pipes.size == expectedSize, "Cannot apply an operation for " + expectedSize + "pipes to " + pipes.size + " pipes. " +
        "Verify matching of given and when clauses in test case definition")

    def apply(pipes: List[TypedPipe[_]]): TypedPipe[TypeOut]
  }

  class OneTypedPipeOperation[TypeIn, TypeOut](op: TypedPipe[TypeIn] => TypedPipe[TypeOut]) extends TypedPipeOperation[TypeOut] {
    override def apply(pipes: List[TypedPipe[_]]): TypedPipe[TypeOut] = {
      assertPipeSize(pipes, 1)
      op(pipes.head.asInstanceOf[TypedPipe[TypeIn]])
    }
  }

  class TwoTypedPipesOperation[TypeIn1, TypeIn2, TypeOut](op: (TypedPipe[TypeIn1], TypedPipe[TypeIn2]) => TypedPipe[TypeOut]) extends TypedPipeOperation[TypeOut] {
    override def apply(pipes: List[TypedPipe[_]]): TypedPipe[TypeOut] = {
      assertPipeSize(pipes, 2)
      op(
        pipes(0).asInstanceOf[TypedPipe[TypeIn1]], // linter:ignore
        pipes(1).asInstanceOf[TypedPipe[TypeIn2]])
    }
  }

  class ThreeTypedPipesOperation[TypeIn1, TypeIn2, TypeIn3, TypeOut](op: (TypedPipe[TypeIn1], TypedPipe[TypeIn2], TypedPipe[TypeIn3]) => TypedPipe[TypeOut]) extends TypedPipeOperation[TypeOut] {
    override def apply(pipes: List[TypedPipe[_]]): TypedPipe[TypeOut] = {
      assertPipeSize(pipes, 3)
      op(
        pipes(0).asInstanceOf[TypedPipe[TypeIn1]], // linter:ignore
        pipes(1).asInstanceOf[TypedPipe[TypeIn2]],
        pipes(2).asInstanceOf[TypedPipe[TypeIn3]])
    }
  }

  class ListOfTypedPipesOperations[TypeOut](op: List[TypedPipe[_]] => TypedPipe[TypeOut]) extends TypedPipeOperation[TypeOut] {
    override def apply(pipes: List[TypedPipe[_]]): TypedPipe[TypeOut] = op(pipes)
  }

  implicit def fromSingleTypedPipeFunctionToOperation[TypeIn, TypeOut](op: TypedPipe[TypeIn] => TypedPipe[TypeOut]): OneTypedPipeOperation[TypeIn, TypeOut] =
    new OneTypedPipeOperation[TypeIn, TypeOut](op)

  implicit def fromTwoTypedPipesFunctionToOperation[TypeIn1, TypeIn2, TypeOut](op: (TypedPipe[TypeIn1], TypedPipe[TypeIn2]) => TypedPipe[TypeOut]): TwoTypedPipesOperation[TypeIn1, TypeIn2, TypeOut] =
    new TwoTypedPipesOperation[TypeIn1, TypeIn2, TypeOut](op)

  implicit def fromThreeTypedPipesFunctionToOperation[TypeIn1, TypeIn2, TypeIn3, TypeOut](op: (TypedPipe[TypeIn1], TypedPipe[TypeIn2], TypedPipe[TypeIn3]) => TypedPipe[TypeOut]): ThreeTypedPipesOperation[TypeIn1, TypeIn2, TypeIn3, TypeOut] =
    new ThreeTypedPipesOperation[TypeIn1, TypeIn2, TypeIn3, TypeOut](op)

  implicit def fromListOfTypedPipesFunctionToOperation[TypeOut](op: List[TypedPipe[_]] => TypedPipe[TypeOut]): ListOfTypedPipesOperations[TypeOut] =
    new ListOfTypedPipesOperations[TypeOut](op)
}
