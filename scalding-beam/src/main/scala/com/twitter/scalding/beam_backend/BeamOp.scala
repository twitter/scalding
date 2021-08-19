package com.twitter.scalding.beam_backend

import com.twitter.scalding.Config
import com.twitter.scalding.beam_backend.BeamFunctions._
import com.twitter.scalding.typed.TypedSource
import java.lang
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.{ IterableCoder, KvCoder }
import org.apache.beam.sdk.extensions.sorter.{ BufferedExternalSorter, SortValues }
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.values.{ KV, PCollection }
import scala.collection.JavaConverters._
import scala.language.implicitConversions

sealed abstract class BeamOp[+A] {
  import BeamOp.TransformBeamOp

  def run(pipeline: Pipeline): PCollection[_ <: A]

  def map[B](f: A => B)(implicit kryoCoder: KryoCoder): BeamOp[B] =
    parDo(MapFn(f))

  def filter(f: A => Boolean)(implicit kryoCoder: KryoCoder): BeamOp[A] =
    applyPTransform(Filter.by[A, ProcessFunction[A, java.lang.Boolean]](ProcessPredicate(f)))

  def applyPTransform[C >: A, B](f: PTransform[PCollection[C], PCollection[B]])(implicit kryoCoder: KryoCoder): BeamOp[B] =
    TransformBeamOp(this, f, kryoCoder)

  def flatMap[B](f: A => TraversableOnce[B])(implicit kryoCoder: KryoCoder): BeamOp[B] =
    parDo(FlatMapFn(f))

  def parDo[C >: A, B](f: DoFn[C, B])(implicit kryoCoder: KryoCoder): BeamOp[B] = {
    val pTransform = new PTransform[PCollection[C], PCollection[B]]() {
      override def expand(input: PCollection[C]): PCollection[B] = input.apply(ParDo.of(f))
    }
    applyPTransform(pTransform)
  }
}

object BeamOp extends Serializable {
  final case class Source[A](
    conf: Config,
    original: TypedSource[A],
    input: Option[BeamSource[A]]) extends BeamOp[A] {
    def run(pipeline: Pipeline): PCollection[_ <: A] =
      input match {
        case None => throw new IllegalArgumentException(s"source $original was not connected to a beam source")
        case Some(src) => src.read(pipeline, conf)
      }
  }

  final case class FromIterable[A](iterable: Iterable[A], kryoCoder: KryoCoder) extends BeamOp[A] {
    override def run(pipeline: Pipeline): PCollection[_ <: A] = {
      pipeline.apply(Create.of(iterable.asJava).withCoder(kryoCoder))
    }
  }

  final case class TransformBeamOp[A, B](source: BeamOp[A], f: PTransform[PCollection[A], PCollection[B]], kryoCoder: KryoCoder) extends BeamOp[B] {
    def run(pipeline: Pipeline): PCollection[B] = {
      source.run(pipeline).asInstanceOf[PCollection[A]].apply(f).setCoder(kryoCoder)
    }
  }

  implicit class KVOp[K, V](val op: BeamOp[(K, V)]) extends AnyVal {
    def mapGroup[U](reduceFn: (K, Iterator[V]) => Iterator[U])(implicit ordK: Ordering[K], kryoCoder: KryoCoder): BeamOp[(K, U)] = {
      TransformBeamOp[(K, V), (K, U)](
        op,
        new PTransform[PCollection[(K, V)], PCollection[(K, U)]]() {
          override def expand(input: PCollection[(K, V)]): PCollection[(K, U)] = {
            input
              .apply(new TupleToKV[K, V])
              .apply(GroupByKey.create[K, V]()).setCoder(KvCoder.of(kryoCoder, kryoCoder))
              .apply(ParDo.of(FlatMapFn[KV[K, java.lang.Iterable[V]], KV[K, U]] { elem =>
                reduceFn(elem.getKey, elem.getValue.asScala.toIterator).map(KV.of(elem.getKey, _))
              })).setCoder(KvCoder.of(kryoCoder, kryoCoder))
              .apply(new KVToTuple[K, U])
          }
        },
        kryoCoder)
    }
    def sortedMapGroup[U](reduceFn: (K, Iterator[V]) => Iterator[U])(implicit ordK: Ordering[K], ordV: Ordering[V], kryoCoder: KryoCoder): BeamOp[(K, U)] = {
      TransformBeamOp[(K, V), (K, U)](
        op,
        new PTransform[PCollection[(K, V)], PCollection[(K, U)]]() {
          override def expand(input: PCollection[(K, V)]): PCollection[(K, U)] = {
            input
              .apply(new TupleToKV[K, V])
              .apply(GroupByKey.create[K, V]()).setCoder(KvCoder.of(kryoCoder, kryoCoder))
              .apply(new SortGroupedValues[K, V])
              .apply(ParDo.of(FlatMapFn[KV[K, java.lang.Iterable[V]], KV[K, U]] { elem =>
                reduceFn(elem.getKey, elem.getValue.asScala.toIterator).map(KV.of(elem.getKey, _))
              })).setCoder(KvCoder.of(kryoCoder, kryoCoder))
              .apply(new KVToTuple[K, U])
          }
        },
        kryoCoder)
    }
    def sorted(implicit ordK: Ordering[K], ordV: Ordering[V], kryoCoder: KryoCoder): BeamOp[(K, V)] = {
      TransformBeamOp[(K, V), (K, V)](
        op,
        new PTransform[PCollection[(K, V)], PCollection[(K, V)]]() {
          override def expand(input: PCollection[(K, V)]): PCollection[(K, V)] = {
            input
              .apply(new TupleToKV[K, V])
              .apply(GroupByKey.create[K, V]()).setCoder(KvCoder.of(kryoCoder, kryoCoder))
              .apply(new SortGroupedValues[K, V])
              .apply(ParDo.of(FlatMapFn[KV[K, java.lang.Iterable[V]], KV[K, V]] { elem =>
                elem.getValue.asScala.map(x => KV.of(elem.getKey, x))
              })).setCoder(KvCoder.of(kryoCoder, kryoCoder))
              .apply(new KVToTuple[K, V])
          }
        },
        kryoCoder)
    }
  }

  /**
   * @note we are just ignoring value ordering and using just default ordering provided by
   *       [[BufferedExternalSorter]], so we can't take custom ordering with it
   * @see [[org.apache.beam.sdk.extensions.sorter.Sorter]]
   * @see [[https://beam.apache.org/documentation/sdks/java-extensions/#sorter]]
   * @todo accept ordering of values, maybe sort them in memory, but it could cause out-of-memory exceptions
   */
  case class SortGroupedValues[K, V](implicit ordV: Ordering[V], kryoCoder: KryoCoder) extends PTransform[PCollection[KV[K, java.lang.Iterable[V]]], PCollection[KV[K, java.lang.Iterable[V]]]] {
    override def expand(input: PCollection[KV[K, lang.Iterable[V]]]): PCollection[KV[K, lang.Iterable[V]]] = {
      input
        .apply(ParDo.of(MapFn[KV[K, java.lang.Iterable[V]], KV[K, java.lang.Iterable[KV[V, Unit]]]]{ elem =>
          KV.of(elem.getKey, elem.getValue.asScala.map(KV.of(_, ())).asJava)
        })).setCoder(KvCoder.of(kryoCoder, IterableCoder.of(KvCoder.of(kryoCoder, kryoCoder))))
        .apply(SortValues.create[K, V, Unit](BufferedExternalSorter.options()))
        .apply(ParDo.of(MapFn[KV[K, java.lang.Iterable[KV[V, Unit]]], KV[K, java.lang.Iterable[V]]]{ elem =>
          KV.of(elem.getKey, elem.getValue.asScala.map(_.getKey).asJava)
        })).setCoder(KvCoder.of(kryoCoder, IterableCoder.of(kryoCoder)))
    }
  }

  case class TupleToKV[K, V](implicit kryoCoder: KryoCoder) extends PTransform[PCollection[(K, V)], PCollection[KV[K, V]]] {
    override def expand(input: PCollection[(K, V)]): PCollection[KV[K, V]] = {
      input
        .apply(MapElements.via[(K, V), KV[K, V]](new SimpleFunction[(K, V), KV[K, V]]() {
          override def apply(input: (K, V)): KV[K, V] = KV.of(input._1, input._2)
        })).setCoder(KvCoder.of(kryoCoder, kryoCoder))
    }
  }

  case class KVToTuple[K, V](implicit kryoCoder: KryoCoder) extends PTransform[PCollection[KV[K, V]], PCollection[(K, V)]] {
    override def expand(input: PCollection[KV[K, V]]): PCollection[(K, V)] = {
      input
        .apply(MapElements.via[KV[K, V], (K, V)](new SimpleFunction[KV[K, V], (K, V)]() {
          override def apply(input: KV[K, V]): (K, V) = (input.getKey, input.getValue)
        })).setCoder(kryoCoder)
    }
  }
}

