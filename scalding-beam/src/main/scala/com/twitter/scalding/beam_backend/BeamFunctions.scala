package com.twitter.scalding.beam_backend

import com.twitter.algebird.{Semigroup, SummingCache}
import org.apache.beam.sdk.transforms.DoFn.{FinishBundle, ProcessElement, StartBundle}
import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, GlobalWindow}
import org.apache.beam.sdk.transforms.{DoFn, ProcessFunction}
import org.apache.beam.sdk.values.{PCollection, PCollectionView}
import scala.collection.JavaConverters._

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

  case class MapSideAggregator[K, V](
    size: Int, semigroup: Semigroup[V]
  ) extends DoFn[(K, V), (K, V)] {
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

  case class HashJoinFn[K, V, U, W](
    joiner: (K, V, Iterable[U]) => Iterator[W],
    sideInput: PCollectionView[java.util.Map[K, java.lang.Iterable[U]]]
  ) extends DoFn[(K, V), (K, W)] {
    private[this] var mapRight: java.util.Map[K, java.lang.Iterable[U]] = null
    private[this] val emptyUs: Iterable[U] = Seq.empty[U]

    @ProcessElement
    def processElement(c: DoFn[(K, V), (K, W)]#ProcessContext): Unit = {
      if (mapRight == null) {
        mapRight = c.sideInput(sideInput)
      }
      val key = c.element()._1
      val value = c.element()._2
      val it = mapRight.get(key) match {
        case null => joiner(key, value, emptyUs)
        case notEmpty => joiner(key, value, notEmpty.asScala)
      }
      while (it.hasNext) {
        c.output((key, it.next()))
      }
    }

    @FinishBundle
    def finishBundle(c: DoFn[(K, V), (K, W)]#FinishBundleContext): Unit = {
      mapRight = null
    }
  }

  def widenPCollection[A, B >: A](p: PCollection[_ <: A]): PCollection[B] = p.asInstanceOf[PCollection[B]]
}
