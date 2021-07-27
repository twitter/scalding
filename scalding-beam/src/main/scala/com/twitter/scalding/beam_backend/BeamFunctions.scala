package com.twitter.scalding.beam_backend

import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{ DoFn, ProcessFunction }

object BeamFunctions {
  case class ProcessPredicate[A](f: A => Boolean) extends ProcessFunction[A, java.lang.Boolean] {
    @throws[Exception]
    override def apply(input: A): java.lang.Boolean = java.lang.Boolean.valueOf(f(input))
  }

  case class FlatMapFn[A, B](f: A => TraversableOnce[B]) extends DoFn[A, B] {
    @ProcessElement
    def processElement(c: DoFn[A, B]#ProcessContext): Unit = {
      val it = f(c.element()).toIterator
      while (it.hasNext) c.output(it.next())
    }
  }

  case class MapFn[A, B](f: A => B) extends DoFn[A, B] {
    @ProcessElement
    def processElement(c: DoFn[A, B]#ProcessContext): Unit =
      c.output(f(c.element()))
  }
}
