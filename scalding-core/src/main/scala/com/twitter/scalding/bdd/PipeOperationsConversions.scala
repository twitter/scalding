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
      assertPipeSize(pipes, 1); op(pipes(0))
    }
  }

  class TwoPipesOperation(op: (RichPipe, Pipe) => RichPipe) extends PipeOperation {
    def apply(pipes: List[RichPipe]): Pipe = {
      assertPipeSize(pipes, 2); op(pipes(0), pipes(1))
    }
  }

  class ThreePipesOperation(op: (RichPipe, RichPipe, RichPipe) => Pipe) extends PipeOperation {
    def apply(pipes: List[RichPipe]): Pipe = {
      assertPipeSize(pipes, 3); op(pipes(0), pipes(1), pipes(2))
    }
  }

  class ListRichPipesOperation(op: List[RichPipe] => Pipe) extends PipeOperation {
    def apply(pipes: List[RichPipe]): Pipe = op(pipes)
  }

  class ListPipesOperation(op: List[Pipe] => Pipe) extends PipeOperation {
    def apply(pipes: List[RichPipe]): Pipe = op(pipes.map(_.pipe).toList)
  }

  implicit val fromSingleRichPipeFunctionToOperation = (op: RichPipe => RichPipe) => new OnePipeOperation(op(_).pipe)
  implicit val fromSingleRichPipeToPipeFunctionToOperation = (op: RichPipe => Pipe) => new OnePipeOperation(op(_))

  implicit val fromTwoRichPipesFunctionToOperation = (op: (RichPipe, RichPipe) => RichPipe) => new TwoPipesOperation(op(_, _).pipe)
  implicit val fromTwoRichPipesToRichPipeFunctionToOperation = (op: (RichPipe, RichPipe) => Pipe) => new TwoPipesOperation(op(_, _))

  implicit val fromThreeRichPipesFunctionToOperation = (op: (RichPipe, RichPipe, RichPipe) => RichPipe) => new ThreePipesOperation(op(_, _, _).pipe)
  implicit val fromThreeRichPipesToPipeFunctionToOperation = (op: (RichPipe, RichPipe, RichPipe) => Pipe) => new ThreePipesOperation(op(_, _, _))

  implicit val fromRichPipeListFunctionToOperation = (op: List[RichPipe] => RichPipe) => new ListRichPipesOperation(op(_).pipe)
  implicit val fromRichPipeListToPipeFunctionToOperation = (op: List[RichPipe] => Pipe) => new ListRichPipesOperation(op(_))

  implicit val fromSinglePipeFunctionToOperation = (op: Pipe => RichPipe) => new OnePipeOperation(op(_).pipe)
  implicit val fromSinglePipeToRichPipeFunctionToOperation = (op: Pipe => Pipe) => new OnePipeOperation(op(_))

  implicit val fromTwoPipeFunctionToOperation = (op: (Pipe, Pipe) => RichPipe) => new TwoPipesOperation(op(_, _).pipe)
  implicit val fromTwoRichPipeToPipeFunctionToOperation = (op: (Pipe, Pipe) => Pipe) => new TwoPipesOperation(op(_, _))

  implicit val fromThreePipeFunctionToOperation = (op: (Pipe, Pipe, Pipe) => RichPipe) => new ThreePipesOperation(op(_, _, _).pipe)
  implicit val fromThreeRichPipeToPipeFunctionToOperation = (op: (Pipe, Pipe, Pipe) => Pipe) => new ThreePipesOperation(op(_, _, _))

  implicit val fromListPipeFunctionToOperation = (op: List[Pipe] => RichPipe) => new ListPipesOperation(op(_).pipe)
  implicit val fromListRichPipeToPipeFunctionToOperation = (op: List[Pipe] => Pipe) => new ListPipesOperation(op(_))
}
