package com.twitter.scalding.beam_backend

import com.twitter.algebird.{ Semigroup, SummingCache }
import org.apache.beam.sdk.transforms.DoFn.{ FinishBundle, ProcessElement, StartBundle }
import org.apache.beam.sdk.transforms.windowing.{ BoundedWindow, GlobalWindow }
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

  case class MapSideAggregator[K, V](size: Int, semigroup: Semigroup[V]) extends DoFn[(K, V), (K, V)] {
    var cache: SummingCache[K, V] = _
    @StartBundle
    def startBundle(): Unit = {
      cache = new SummingCache[K, V](size)(semigroup)
    }

    @ProcessElement
    def processElement(c: DoFn[(K, V), (K, V)]#ProcessContext): Unit = {
      val evicted = cache.put(Map(c.element()))
      evicted match {
        case Some(m) =>
          val mit = m.iterator
          while (mit.hasNext) {
            c.output(mit.next())
          }
        case None => ()
      }
    }

    @FinishBundle
    def finishBundle(c: DoFn[(K, V), (K, V)]#FinishBundleContext): Unit = {
      val evicted = cache.flush
      evicted match {
        case Some(m) =>
          val mit = m.iterator
          while (mit.hasNext) {
            c.output(mit.next(), BoundedWindow.TIMESTAMP_MIN_VALUE, GlobalWindow.INSTANCE)
          }
        case None => ()
      }
    }
  }
}
