package com.twitter.scalding.beam_backend

import com.twitter.algebird.Semigroup
import com.twitter.scalding.Config
import com.twitter.scalding.beam_backend.BeamFunctions._
import com.twitter.scalding.typed.functions.ComposedFunctions.ComposedMapGroup
import com.twitter.scalding.typed.functions.{EmptyGuard, MapValueStream, SumAll}
import com.twitter.scalding.typed.{CoGrouped, TypedSource}
import java.lang
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.{Coder, IterableCoder, KvCoder}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.transforms.join.{
  CoGbkResult,
  CoGbkResultSchema,
  CoGroupByKey,
  KeyedPCollectionTuple,
  UnionCoder
}
import org.apache.beam.sdk.values.{KV, PCollection, TupleTag}
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

  def applyPTransform[C >: A, B](
    f: PTransform[PCollection[C], PCollection[B]]
  )(implicit kryoCoder: KryoCoder): BeamOp[B] =
    TransformBeamOp(this, f, kryoCoder)

  def flatMap[B](f: A => TraversableOnce[B])(implicit kryoCoder: KryoCoder): BeamOp[B] =
    parDo(FlatMapFn(f))
}

object BeamOp extends Serializable {
  implicit private def fakeClassTag[A]: ClassTag[A] = ClassTag(classOf[AnyRef]).asInstanceOf[ClassTag[A]]

  def planMapGroup[K, V, U](
    pcoll: PCollection[KV[K, java.lang.Iterable[V]]],
    reduceFn: (K, Iterator[V]) => Iterator[U]
  )(implicit ordK: Ordering[K], kryoCoder: KryoCoder): PCollection[KV[K, java.lang.Iterable[U]]] = {
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
          })).setCoder(
          KvCoder.of(OrderedSerializationCoder(ordK, kryoCoder), IterableCoder.of(kryoCoder))
        )
      case notComposedOrSum =>
        pcoll.apply(ParDo.of(
          MapFn[KV[K, java.lang.Iterable[V]], KV[K, java.lang.Iterable[U]]] { elem =>
            KV.of(
              elem.getKey,
              notComposedOrSum(elem.getKey, elem.getValue.asScala.toIterator).toIterable.asJava)
          })).setCoder(
          KvCoder.of(OrderedSerializationCoder(ordK, kryoCoder), IterableCoder.of(kryoCoder)))
    }
  }

  final case class Source[A](
    conf: Config,
    original: TypedSource[A],
    input: Option[BeamSource[A]]) extends BeamOp[A] {
    def run(pipeline: Pipeline): PCollection[_ <: A] =
      input match {
        case None => throw new IllegalArgumentException(
          s"source $original was not connected to a beam source"
        )
        case Some(src) => src.read(pipeline, conf)
      }
  }

  final case class FromIterable[A](iterable: Iterable[A], kryoCoder: KryoCoder) extends BeamOp[A] {
    override def run(pipeline: Pipeline): PCollection[_ <: A] = {
      pipeline.apply(Create.of(iterable.asJava).withCoder(kryoCoder))
    }
  }

  final case class TransformBeamOp[A, B](
    source: BeamOp[A],
    f: PTransform[PCollection[A], PCollection[B]],
    kryoCoder: KryoCoder
  ) extends BeamOp[B] {
    def run(pipeline: Pipeline): PCollection[B] = {
      val pCollection: PCollection[A] = widenPCollection(source.run(pipeline))
      pCollection.apply(f).setCoder(kryoCoder)
    }
  }

  final case class HashJoinOp[K, V, U, W](
    left: BeamOp[(K, V)],
    right: BeamOp[(K, U)], joiner: (K, V, Iterable[U]) => Iterator[W]
  )(implicit kryoCoder: KryoCoder, ordK: Ordering[K]) extends BeamOp[(K, W)] {
    override def run(pipeline: Pipeline): PCollection[_ <: (K, W)] = {
      val leftPCollection = left.run(pipeline)
      val keyCoder: Coder[K] = OrderedSerializationCoder.apply(ordK, kryoCoder)
      val rightPCollection: PCollection[(K, U)] = widenPCollection(right.run(pipeline))

      val rightPCollectionView = rightPCollection
        .apply(TupleToKV[K, U](keyCoder, kryoCoder))
        .apply(GroupByKey.create[K, U]()).setCoder(KvCoder.of(keyCoder, kryoCoder))
        .apply(View.asMap[K, java.lang.Iterable[U]]())

      leftPCollection
        .apply(
          ParDo
            .of(HashJoinFn[K, V, U, W](joiner, rightPCollectionView))
            .withSideInputs(rightPCollectionView)
        )
        .setCoder(TupleCoder(keyCoder, kryoCoder))
    }
  }

  final case class CoGroupedOp[K, V](
    cg: CoGrouped[K, V],
    inputOps: Seq[BeamOp[(K, Any)]]
  )(implicit kryoCoder: KryoCoder) extends BeamOp[(K, V)] {
    override def run(pipeline: Pipeline): PCollection[_ <: (K, V)] = {
      val inputOpsSize = inputOps.size
      val keyCoder: Coder[K] = OrderedSerializationCoder.apply(cg.keyOrdering, kryoCoder)
      val pcols = inputOps.map { inputOp =>
        val pCol: PCollection[(K, Any)] = widenPCollection(inputOp.op.run(pipeline))
        pCol.apply(TupleToKV[K, Any](keyCoder, kryoCoder))
      }

      val tupleTags = (1 to inputOpsSize).map(idx => new TupleTag[Any](idx.toString))

      val unionCoder = UnionCoder.of(
        (1 to inputOpsSize).map(_ => kryoCoder.asInstanceOf[Coder[_]]).asJava
      )

      val coGroupResultCoder: CoGbkResult.CoGbkResultCoder = CoGbkResult.CoGbkResultCoder.of(
        CoGbkResultSchema.of(tupleTags.map(_.asInstanceOf[TupleTag[_]]).asJava),
        unionCoder
      )

      val keyedPCollectionTuple: KeyedPCollectionTuple[K] = pcols
        .zip(tupleTags)
        .foldLeft(
          KeyedPCollectionTuple.empty[K](pipeline)
        )((keyed, colWithTag) => keyed.and[Any](colWithTag._2, colWithTag._1))

      keyedPCollectionTuple
        .apply(CoGroupByKey.create()).setCoder(KvCoder.of(keyCoder, coGroupResultCoder))
        .apply(ParDo.of(new CoGroupDoFn[K, V](cg, tupleTags)))
        .setCoder(KvCoder.of(keyCoder, kryoCoder))
        .apply(KVToTuple[K, V](keyCoder, kryoCoder))
    }
  }

  case class CoGroupDoFn[K, V](
    coGrouped: CoGrouped[K, V],
    tags: Seq[TupleTag[Any]]
  ) extends DoFn[KV[K, CoGbkResult], KV[K, V]] {
    @ProcessElement
    def processElement(c: DoFn[KV[K, CoGbkResult], KV[K, V]]#ProcessContext): Unit = {
      val key = c.element().getKey
      val value = c.element().getValue
      val iteratorSeq: Seq[lang.Iterable[Any]] = tags.map(t => value.getAll(t))

      val outputIter = coGrouped.joinFunction.apply(
        key,
        iteratorSeq.head.iterator().asScala,
        iteratorSeq.drop(1).map(_.asScala)
      )

      while(outputIter.hasNext) {
        c.output(KV.of(key, outputIter.next()))
      }
    }
  }

  implicit class KVOp[K, V](val op: BeamOp[(K, V)]) extends AnyVal {
    def mapGroup[U](
      reduceFn: (K, Iterator[V]) => Iterator[U]
    )(implicit ordK: Ordering[K], kryoCoder: KryoCoder): BeamOp[(K, U)] = {
      TransformBeamOp[(K, V), (K, U)](
        op,
        new PTransform[PCollection[(K, V)], PCollection[(K, U)]]() {
          override def expand(input: PCollection[(K, V)]): PCollection[(K, U)] = {
            val keyCoder: Coder[K] = OrderedSerializationCoder(ordK, kryoCoder)

            val groupedValues = input
              .apply(TupleToKV[K, V](keyCoder, kryoCoder))
              .apply(GroupByKey.create[K, V]())
              .setCoder(KvCoder.of(keyCoder, IterableCoder.of(kryoCoder)))

            planMapGroup[K, V, U](groupedValues, reduceFn)
              .apply(ParDo.of(FlatMapFn[KV[K, java.lang.Iterable[U]], KV[K, U]]{ elem =>
                elem.getValue.asScala.map(KV.of(elem.getKey, _))
              })).setCoder(KvCoder.of(keyCoder, kryoCoder))
              .apply(KVToTuple[K, U](keyCoder, kryoCoder))
          }
        },
        kryoCoder)
    }

    def sortedMapGroup[U](
      reduceFn: (K, Iterator[V]) => Iterator[U]
    )(implicit ordK: Ordering[K], ordV: Ordering[V], kryoCoder: KryoCoder): BeamOp[(K, U)] = {
      TransformBeamOp[(K, V), (K, U)](
        op,
        new PTransform[PCollection[(K, V)], PCollection[(K, U)]]() {
          override def expand(input: PCollection[(K, V)]): PCollection[(K, U)] = {
            val keyCoder: Coder[K] = OrderedSerializationCoder(ordK, kryoCoder)
            val valueCoder: Coder[V] = OrderedSerializationCoder(ordV, kryoCoder)

            val groupedSortedValues = input
              .apply(TupleToKV[K, V](keyCoder, valueCoder))
              .apply(GroupByKey.create[K, V]())
              .setCoder(KvCoder.of(keyCoder, IterableCoder.of(valueCoder)))
              .apply(SortGroupedValues[K, V])

            planMapGroup[K, V, U](groupedSortedValues, reduceFn)
              .apply(ParDo.of(FlatMapFn[KV[K, java.lang.Iterable[U]], KV[K, U]]{ elem =>
                elem.getValue.asScala.map(KV.of(elem.getKey, _))
              })).setCoder(KvCoder.of(keyCoder, kryoCoder))
              .apply(KVToTuple[K, U](keyCoder, kryoCoder))
          }
        },
        kryoCoder)
    }

    def sorted(implicit ordK: Ordering[K], ordV: Ordering[V], kryoCoder: KryoCoder): BeamOp[(K, V)] = {
      TransformBeamOp[(K, V), (K, V)](
        op,
        new PTransform[PCollection[(K, V)], PCollection[(K, V)]]() {
          override def expand(input: PCollection[(K, V)]): PCollection[(K, V)] = {
            val keyCoder: Coder[K] = OrderedSerializationCoder(ordK, kryoCoder)
            val valueCoder: Coder[V] = OrderedSerializationCoder(ordV, kryoCoder)
            input
              .apply(TupleToKV[K, V](keyCoder, valueCoder))
              .apply(GroupByKey.create[K, V]())
              .setCoder(KvCoder.of(keyCoder, IterableCoder.of(valueCoder)))
              .apply(SortGroupedValues[K, V])
              .apply(ParDo.of(FlatMapFn[KV[K, java.lang.Iterable[V]], KV[K, V]] { elem =>
                elem.getValue.asScala.map(x => KV.of(elem.getKey, x))
              }))
              .setCoder(KvCoder.of(keyCoder, valueCoder))
              .apply(KVToTuple[K, V](keyCoder, valueCoder))
          }
        },
        kryoCoder)
    }

    def mapSideAggregator(
      size: Int, semigroup: Semigroup[V]
    )(implicit kryoCoder: KryoCoder): BeamOp[(K, V)] = {
      TransformBeamOp[(K, V), (K, V)](
        op,
        new PTransform[PCollection[(K, V)], PCollection[(K, V)]]() {
          override def expand(input: PCollection[(K, V)]): PCollection[(K, V)] =
            input.apply(ParDo.of(MapSideAggregator[K, V](size, semigroup))).setCoder(kryoCoder)
        },
        kryoCoder)
    }

    def hashJoin[U, W](
      right: BeamOp[(K, U)],
      fn: (K, V, Iterable[U]) => Iterator[W]
    )(implicit kryoCoder: KryoCoder, ord: Ordering[K]): BeamOp[(K, W)] = {
      HashJoinOp(op, right, fn)
    }
  }

  /**
   * @todo this needs to be changed to some external sorter, current Beam external sorter
   *       implementation does not provide an option to sort with custom Ordering
   * @see [[org.apache.beam.sdk.extensions.sorter.ExternalSorter]]
   */
  case class SortGroupedValues[K, V](
    implicit ordK: Ordering[K], ordV: Ordering[V], kryoCoder: KryoCoder
  ) extends PTransform[PCollection[KV[K, java.lang.Iterable[V]]], PCollection[KV[K, java.lang.Iterable[V]]]] {
    override def expand(
      input: PCollection[KV[K, lang.Iterable[V]]]
    ): PCollection[KV[K, lang.Iterable[V]]] = {
      input
        .apply(ParDo.of(MapFn[KV[K, java.lang.Iterable[V]], KV[K, java.lang.Iterable[V]]]{ elem =>
          KV.of(elem.getKey, elem.getValue.asScala.toArray.sorted.toIterable.asJava)
        }))
        .setCoder(KvCoder.of(
          OrderedSerializationCoder(ordK, kryoCoder),
          IterableCoder.of(OrderedSerializationCoder(ordV, kryoCoder)))
        )
    }
  }

  case class TupleToKV[K, V](
    kCoder: Coder[K],
    vCoder: Coder[V]
  ) extends PTransform[PCollection[(K, V)], PCollection[KV[K, V]]] {
    override def expand(input: PCollection[(K, V)]): PCollection[KV[K, V]] = {
      input
        .apply(MapElements.via[(K, V), KV[K, V]](new SimpleFunction[(K, V), KV[K, V]]() {
          override def apply(input: (K, V)): KV[K, V] = KV.of(input._1, input._2)
        })).setCoder(KvCoder.of(kCoder, vCoder))
    }
  }

  case class KVToTuple[K, V](
    coderK: Coder[K],
    coderV: Coder[V]
  ) extends PTransform[PCollection[KV[K, V]], PCollection[(K, V)]] {
    override def expand(input: PCollection[KV[K, V]]): PCollection[(K, V)] = {
      input
        .apply(MapElements.via[KV[K, V], (K, V)](new SimpleFunction[KV[K, V], (K, V)]() {
          override def apply(input: KV[K, V]): (K, V) = (input.getKey, input.getValue)
        })).setCoder(TupleCoder(coderK, coderV))
    }
  }
}

