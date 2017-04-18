package com.twitter.example.scalding.typed

import com.twitter.scalding._
import scala.concurrent.{ ExecutionContext => SExecutionContext, _ }
import SExecutionContext.Implicits.global

object InAnotherPackage {
  def buildF: Future[TypedPipe[(Int, Int)]] = {
    Future {
      val p = TypedPipe.from(List(1, 2, 3, 4, 555, 3))
        .map { case x => (x, x) }

      // TODO this shouldn't be needed. In a following
      // PR, this should be removed if we merge this
      // change
      LineNumber.tryNonScaldingCaller match {
        case None => p
        case Some(c) => p.withDescription(c.toString)
      }
    }
  }
}
