package com.twitter.example.scalding.typed

import com.twitter.scalding._
import scala.concurrent.{ ExecutionContext => SExecutionContext, _ }
import SExecutionContext.Implicits.global

object InAnotherPackage {
  /* this object is used in NoStackLineNumberTest */

  def buildF: Future[TypedPipe[(Int, Int)]] = {
    Future {
      TypedPipe.from(List(1, 2, 3, 4, 555, 3))
        .map { case x => (x, x) }
    }
  }
}
