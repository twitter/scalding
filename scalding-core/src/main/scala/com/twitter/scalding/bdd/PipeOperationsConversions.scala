package com.twitter.scalding.bdd

import com.twitter.scalding.{ Dsl, RichPipe }
import cascading.pipe.Pipe

trait PipeOperationsConversions {
  import Dsl._

  trait PipeOperation {
    def assertPipeSize(pipes: List[RichPipe], expectedSize: Int) =
      require(pipes.size == expectedSize, "Cannot apply an operation for " + expectedSize + "pipes to " + pipes.size + " pipes. " +
        "Verify matching of given and when clauses in test case definition")

    def apply(pipes: List[RichPipe]): Pipe
  }

  class OnePipeOperation(op: RichPipe => Pipe) extends PipeOperation {
    def apply(pipes: List[RichPipe]): Pipe = {
      assertPipeSize(pipes, 1); op(pipes.head)
    }
  }

  class TwoPipesOperation(op: (RichPipe, Pipe) => RichPipe) extends PipeOperation {
    def apply(pipes: List[RichPipe]): Pipe = {
      assertPipeSize(pipes, 2); op(pipes(0), pipes(1)) // linter:ignore
    }
  }

  class ThreePipesOperation(op: (RichPipe, RichPipe, RichPipe) => Pipe) extends PipeOperation {
    def apply(pipes: List[RichPipe]): Pipe = {
      assertPipeSize(pipes, 3); op(pipes(0), pipes(1), pipes(2)) // linter:ignore
    }
  }

  class ListRichPipesOperation(op: List[RichPipe] => Pipe) extends PipeOperation {
    def apply(pipes: List[RichPipe]): Pipe = op(pipes)
  }

  class ListPipesOperation(op: List[Pipe] => Pipe) extends PipeOperation {
    def apply(pipes: List[RichPipe]): Pipe = op(pipes.map(_.pipe).toList)
  }

  implicit val fromSingleRichPipeFunctionToOperation: (RichPipe => RichPipe) => OnePipeOperation = (op: RichPipe => RichPipe) => new OnePipeOperation(op(_).pipe)
  implicit val fromSingleRichPipeToPipeFunctionToOperation: (RichPipe => Pipe) => OnePipeOperation = (op: RichPipe => Pipe) => new OnePipeOperation(op(_))

  implicit val fromTwoRichPipesFunctionToOperation: ((RichPipe, RichPipe) => RichPipe) => TwoPipesOperation = (op: (RichPipe, RichPipe) => RichPipe) => new TwoPipesOperation(op(_, _).pipe)
  implicit val fromTwoRichPipesToRichPipeFunctionToOperation: ((RichPipe, RichPipe) => Pipe) => TwoPipesOperation = (op: (RichPipe, RichPipe) => Pipe) => new TwoPipesOperation(op(_, _))

  implicit val fromThreeRichPipesFunctionToOperation: ((RichPipe, RichPipe, RichPipe) => RichPipe) => ThreePipesOperation = (op: (RichPipe, RichPipe, RichPipe) => RichPipe) => new ThreePipesOperation(op(_, _, _).pipe)
  implicit val fromThreeRichPipesToPipeFunctionToOperation: ((RichPipe, RichPipe, RichPipe) => Pipe) => ThreePipesOperation = (op: (RichPipe, RichPipe, RichPipe) => Pipe) => new ThreePipesOperation(op(_, _, _))

  implicit val fromRichPipeListFunctionToOperation: (List[RichPipe] => RichPipe) => ListRichPipesOperation = (op: List[RichPipe] => RichPipe) => new ListRichPipesOperation(op(_).pipe)
  implicit val fromRichPipeListToPipeFunctionToOperation: (List[RichPipe] => Pipe) => ListRichPipesOperation = (op: List[RichPipe] => Pipe) => new ListRichPipesOperation(op(_))

  implicit val fromSinglePipeFunctionToOperation: (Pipe => RichPipe) => OnePipeOperation = (op: Pipe => RichPipe) => new OnePipeOperation(op(_).pipe)
  implicit val fromSinglePipeToRichPipeFunctionToOperation: (Pipe => Pipe) => OnePipeOperation = (op: Pipe => Pipe) => new OnePipeOperation(op(_))

  implicit val fromTwoPipeFunctionToOperation: ((Pipe, Pipe) => RichPipe) => TwoPipesOperation = (op: (Pipe, Pipe) => RichPipe) => new TwoPipesOperation(op(_, _).pipe)
  implicit val fromTwoRichPipeToPipeFunctionToOperation: ((Pipe, Pipe) => Pipe) => TwoPipesOperation = (op: (Pipe, Pipe) => Pipe) => new TwoPipesOperation(op(_, _))

  implicit val fromThreePipeFunctionToOperation: ((Pipe, Pipe, Pipe) => RichPipe) => ThreePipesOperation = (op: (Pipe, Pipe, Pipe) => RichPipe) => new ThreePipesOperation(op(_, _, _).pipe)
  implicit val fromThreeRichPipeToPipeFunctionToOperation: ((Pipe, Pipe, Pipe) => Pipe) => ThreePipesOperation = (op: (Pipe, Pipe, Pipe) => Pipe) => new ThreePipesOperation(op(_, _, _))

  implicit val fromListPipeFunctionToOperation: (List[Pipe] => RichPipe) => ListPipesOperation = (op: List[Pipe] => RichPipe) => new ListPipesOperation(op(_).pipe)
  implicit val fromListRichPipeToPipeFunctionToOperation: (List[Pipe] => Pipe) => ListPipesOperation = (op: List[Pipe] => Pipe) => new ListPipesOperation(op(_))
}
