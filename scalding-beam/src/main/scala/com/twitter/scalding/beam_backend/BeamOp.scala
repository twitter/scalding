package com.twitter.scalding.beam_backend

import com.twitter.algebird.Semigroup
import com.twitter.scalding.Config
import com.twitter.scalding.beam_backend.BeamFunctions._
import com.twitter.scalding.typed.TypedSource
import com.twitter.scalding.typed.functions.ComposedFunctions.ComposedMapGroup
import com.twitter.scalding.typed.functions.{ EmptyGuard, MapValueStream, SumAll }
import java.lang
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.{ Coder, IterableCoder, KvCoder }
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.values.{ KV, PCollection }
import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.reflect.ClassTag

sealed abstract class BeamOp[+A] {
  import BeamOp.TransformBeamOp

  def run(pipeline: Pipeline): PCollection[_ <: A]

  def map[B](f: A => B)(implicit kryoCoder: KryoCoder): BeamOp[B] =
    parDo(MapFn(f))

  def parDo[C >: A, B](f: DoFn[C, B])(implicit kryoCoder: KryoCoder): BeamOp[B] = {
    val pTransform = new PTransform[PCollection[C], PCollection[B]]() {
      override def expand(input: PCollection[C]): PCollection[B] = input.apply(ParDo.of(f))
    }
    applyPTransform(pTransform)
  }

  def filter(f: A => Boolean)(implicit kryoCoder: KryoCoder): BeamOp[A] =
    applyPTransform(Filter.by[A, ProcessFunction[A, java.lang.Boolean]](ProcessPredicate(f)))

  def applyPTransform[C >: A, B](f: PTransform[PCollection[C], PCollection[B]])(implicit kryoCoder: KryoCoder): BeamOp[B] =
    TransformBeamOp(this, f, kryoCoder)

  def flatMap[B](f: A => TraversableOnce[B])(implicit kryoCoder: KryoCoder): BeamOp[B] =
    parDo(FlatMapFn(f))
}

object BeamOp extends Serializable {
  implicit private def fakeClassTag[A]: ClassTag[A] = ClassTag(classOf[AnyRef]).asInstanceOf[ClassTag[A]]

  def planMapGroup[K, V, U](
    pcoll: PCollection[KV[K, java.lang.Iterable[V]]],
    reduceFn: (K, Iterator[V]) => Iterator[U])(implicit ordK: Ordering[K], kryoCoder: KryoCoder): PCollection[KV[K, java.lang.Iterable[U]]] = {
    reduceFn match {
      case ComposedMapGroup(f, g) => planMapGroup(planMapGroup(pcoll, f), g)
      case EmptyGuard(MapValueStream(sa: SumAll[V])) =>
        pcoll
          .apply(Combine.groupedValues(
            new SerializableBiFunction[V, V, V] {
              override def apply(t: V, u: V): V = sa.sg.plus (t, u)
            })).setCoder(KvCoder.of(OrderedSerializationCoder(ordK, kryoCoder), kryoCoder))
          .apply(MapElements.via(new SimpleFunction[KV[K, V], KV[K, java.lang.Iterable[U]]]() {
            override def apply(input: KV[K, V]): KV[K, lang.Iterable[U]] =
              KV.of(input.getKey, Seq(input.getValue.asInstanceOf[U]).toIterable.asJava)
          })).setCoder(KvCoder.of(OrderedSerializationCoder(ordK, kryoCoder), IterableCoder.of(kryoCoder)))
      case notComposedOrSum =>
        pcoll.apply(ParDo.of(
          MapFn[KV[K, java.lang.Iterable[V]], KV[K, java.lang.Iterable[U]]] { elem =>
            KV.of(
              elem.getKey,
              notComposedOrSum(elem.getKey, elem.getValue.asScala.toIterator).toIterable.asJava)
          })).setCoder(KvCoder.of(OrderedSerializationCoder(ordK, kryoCoder), IterableCoder.of(kryoCoder)))
    }
  }

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
            val groupedValues = input
              .apply(TupleToKV[K, V](OrderedSerializationCoder(ordK, kryoCoder), kryoCoder))
              .apply(GroupByKey.create[K, V]()).setCoder(KvCoder.of(OrderedSerializationCoder(ordK, kryoCoder), IterableCoder.of(kryoCoder)))

            planMapGroup[K, V, U](groupedValues, reduceFn)
              .apply(ParDo.of(FlatMapFn[KV[K, java.lang.Iterable[U]], KV[K, U]]{ elem =>
                elem.getValue.asScala.map(KV.of(elem.getKey, _))
              })).setCoder(KvCoder.of(OrderedSerializationCoder(ordK, kryoCoder), kryoCoder))
              .apply(KVToTuple[K, U])
          }
        },
        kryoCoder)
    }

    def sortedMapGroup[U](reduceFn: (K, Iterator[V]) => Iterator[U])(implicit ordK: Ordering[K], ordV: Ordering[V], kryoCoder: KryoCoder): BeamOp[(K, U)] = {
      TransformBeamOp[(K, V), (K, U)](
        op,
        new PTransform[PCollection[(K, V)], PCollection[(K, U)]]() {
          override def expand(input: PCollection[(K, V)]): PCollection[(K, U)] = {
            val groupedSortedValues = input
              .apply(TupleToKV[K, V](OrderedSerializationCoder(ordK, kryoCoder), OrderedSerializationCoder(ordV, kryoCoder)))
              .apply(GroupByKey.create[K, V]()).setCoder(KvCoder.of(OrderedSerializationCoder(ordK, kryoCoder), IterableCoder.of(OrderedSerializationCoder(ordV, kryoCoder))))
              .apply(SortGroupedValues[K, V])

            planMapGroup[K, V, U](groupedSortedValues, reduceFn)
              .apply(ParDo.of(FlatMapFn[KV[K, java.lang.Iterable[U]], KV[K, U]]{ elem =>
                elem.getValue.asScala.map(KV.of(elem.getKey, _))
              })).setCoder(KvCoder.of(OrderedSerializationCoder(ordK, kryoCoder), kryoCoder))
              .apply(KVToTuple[K, U])
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
              .apply(TupleToKV[K, V](OrderedSerializationCoder(ordK, kryoCoder), OrderedSerializationCoder(ordV, kryoCoder)))
              .apply(GroupByKey.create[K, V]()).setCoder(KvCoder.of(OrderedSerializationCoder(ordK, kryoCoder), IterableCoder.of(OrderedSerializationCoder(ordV, kryoCoder))))
              .apply(SortGroupedValues[K, V])
              .apply(ParDo.of(FlatMapFn[KV[K, java.lang.Iterable[V]], KV[K, V]] { elem =>
                elem.getValue.asScala.map(x => KV.of(elem.getKey, x))
              })).setCoder(KvCoder.of(OrderedSerializationCoder(ordK, kryoCoder), OrderedSerializationCoder(ordV, kryoCoder)))
              .apply(KVToTuple[K, V])
          }
        },
        kryoCoder)
    }

    def mapSideAggregator(size: Int, semigroup: Semigroup[V])(implicit kryoCoder: KryoCoder): BeamOp[(K, V)] = {
      TransformBeamOp[(K, V), (K, V)](
        op,
        new PTransform[PCollection[(K, V)], PCollection[(K, V)]]() {
          override def expand(input: PCollection[(K, V)]): PCollection[(K, V)] =
            input.apply(ParDo.of(MapSideAggregator[K, V](size, semigroup))).setCoder(kryoCoder)
        },
        kryoCoder)
    }
  }

  /**
   * @todo this needs to be changed to some external sorter, current Beam external sorter implementation
   *       does not provide an option to sort with custom Ordering
   * @see [[org.apache.beam.sdk.extensions.sorter.ExternalSorter]]
   */
  case class SortGroupedValues[K, V](implicit ordK: Ordering[K], ordV: Ordering[V], kryoCoder: KryoCoder) extends PTransform[PCollection[KV[K, java.lang.Iterable[V]]], PCollection[KV[K, java.lang.Iterable[V]]]] {
    override def expand(input: PCollection[KV[K, lang.Iterable[V]]]): PCollection[KV[K, lang.Iterable[V]]] = {
      input
        .apply(ParDo.of(MapFn[KV[K, java.lang.Iterable[V]], KV[K, java.lang.Iterable[V]]]{ elem =>
          KV.of(elem.getKey, elem.getValue.asScala.toArray.sorted.toIterable.asJava)
        })).setCoder(KvCoder.of(OrderedSerializationCoder(ordK, kryoCoder), IterableCoder.of(OrderedSerializationCoder(ordV, kryoCoder))))
    }
  }

  case class TupleToKV[K, V](kCoder: Coder[K], vCoder: Coder[V]) extends PTransform[PCollection[(K, V)], PCollection[KV[K, V]]] {
    override def expand(input: PCollection[(K, V)]): PCollection[KV[K, V]] = {
      input
        .apply(MapElements.via[(K, V), KV[K, V]](new SimpleFunction[(K, V), KV[K, V]]() {
          override def apply(input: (K, V)): KV[K, V] = KV.of(input._1, input._2)
        })).setCoder(KvCoder.of(kCoder, vCoder))
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

