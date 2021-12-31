package com.twitter.scalding.beam_backend

import com.stripe.dagon.Memoize
import com.twitter.algebird.Semigroup
import com.twitter.scalding.Config
import com.twitter.scalding.beam_backend.BeamFunctions._
import com.twitter.scalding.beam_backend.BeamJoiner.MultiJoinFunction
import com.twitter.scalding.serialization.Externalizer
import com.twitter.scalding.typed.functions.ComposedFunctions.ComposedMapGroup
import com.twitter.scalding.typed.functions.{EmptyGuard, MapValueStream, ScaldingPriorityQueueMonoid, SumAll}
import com.twitter.scalding.typed.{CoGrouped, TypedSource}
import java.util.{Comparator, PriorityQueue}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.{Coder, IterableCoder, KvCoder}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.Top.TopCombineFn
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.transforms.join.{CoGbkResult, CoGroupByKey, KeyedPCollectionTuple}
import org.apache.beam.sdk.values.PCollectionList
import org.apache.beam.sdk.values.PCollectionTuple
import org.apache.beam.sdk.values.{KV, PCollection, TupleTag}
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

sealed abstract class BeamOp[+A] {

  import BeamOp.TransformBeamOp

  protected lazy val cachedRun = Memoize.function[Pipeline, PCollection[_ <: A]] { case (pipeline, _) =>
    runNoCache(pipeline)
  }

  final def run(pipeline: Pipeline): PCollection[_ <: A] = cachedRun(pipeline)

  protected def runNoCache(p: Pipeline): PCollection[_ <: A]

  def map[B](f: A => B)(implicit kryoCoder: KryoCoder): BeamOp[B] =
    parDo(MapFn(f), "map")

  def parDo[C >: A, B](f: DoFn[C, B], name: String)(implicit kryoCoder: KryoCoder): BeamOp[B] = {
    val pTransform = new PTransform[PCollection[C], PCollection[B]]() {
      override def expand(input: PCollection[C]): PCollection[B] = input.apply(ParDo.of(f))
    }
    applyPTransform(pTransform, name)
  }

  def filter(f: A => Boolean)(implicit kryoCoder: KryoCoder): BeamOp[A] =
    applyPTransform(Filter.by[A, ProcessFunction[A, java.lang.Boolean]](ProcessPredicate(f)), "filter")

  def applyPTransform[C >: A, B](
      f: PTransform[PCollection[C], PCollection[B]],
      name: String
  )(implicit kryoCoder: KryoCoder): BeamOp[B] =
    TransformBeamOp(this, f, kryoCoder, name)

  def flatMap[B](f: A => TraversableOnce[B])(implicit kryoCoder: KryoCoder): BeamOp[B] =
    parDo(FlatMapFn(f), "flatMap")
}

private final case class SerializableComparator[T](comp: Comparator[T]) extends Comparator[T] {
  private[this] val extCmp = Externalizer(comp)
  override def compare(o1: T, o2: T): Int = extCmp.get.compare(o1, o2)
}

object BeamOp extends Serializable {
  implicit private def fakeClassTag[A]: ClassTag[A] = ClassTag(classOf[AnyRef]).asInstanceOf[ClassTag[A]]

  def planMapGroup[K, V, U](
      pcoll: PCollection[KV[K, java.lang.Iterable[V]]],
      reduceFn: (K, Iterator[V]) => Iterator[U]
  )(implicit ordK: Ordering[K], kryoCoder: KryoCoder): PCollection[KV[K, java.lang.Iterable[U]]] =
    reduceFn match {
      case ComposedMapGroup(f, g) => planMapGroup(planMapGroup(pcoll, f), g)
      case EmptyGuard(MapValueStream(SumAll(pqm: ScaldingPriorityQueueMonoid[v]))) =>
        val vCollection = pcoll.asInstanceOf[PCollection[KV[K, java.lang.Iterable[PriorityQueue[v]]]]]

        vCollection
          .apply(
            MapElements.via(
              new SimpleFunction[
                KV[K, java.lang.Iterable[PriorityQueue[v]]],
                KV[K, java.lang.Iterable[U]]
              ]() {
                private final val topCombineFn = new TopCombineFn[v, SerializableComparator[v]](
                  pqm.count,
                  SerializableComparator[v](pqm.ordering.reverse)
                )

                override def apply(
                    input: KV[K, java.lang.Iterable[PriorityQueue[v]]]
                ): KV[K, java.lang.Iterable[U]] = {
                  @inline def flattenedValues: Stream[v] =
                    input.getValue.asScala.toStream.flatMap(_.asScala.toStream)

                  val outputs: java.util.List[v] = topCombineFn.apply(flattenedValues.asJava)
                  // We are building the PriorityQueue back as output type U is PriorityQueue[v]
                  val pqs = pqm.build(outputs.asScala)
                  KV.of(input.getKey, Iterable(pqs.asInstanceOf[U]).asJava)
                }
              }
            )
          )
          .setCoder(KvCoder.of(OrderedSerializationCoder(ordK, kryoCoder), IterableCoder.of(kryoCoder)))
      case EmptyGuard(MapValueStream(sa: SumAll[V])) =>
        pcoll
          .apply(Combine.groupedValues(new SerializableBiFunction[V, V, V] {
            override def apply(t: V, u: V): V = sa.sg.plus(t, u)
          }))
          .setCoder(KvCoder.of(OrderedSerializationCoder(ordK, kryoCoder), kryoCoder))
          .apply(MapElements.via(new SimpleFunction[KV[K, V], KV[K, java.lang.Iterable[U]]]() {
            override def apply(input: KV[K, V]): KV[K, java.lang.Iterable[U]] =
              KV.of(input.getKey, Seq(input.getValue.asInstanceOf[U]).toIterable.asJava)
          }))
          .setCoder(
            KvCoder.of(OrderedSerializationCoder(ordK, kryoCoder), IterableCoder.of(kryoCoder))
          )
      case notComposedOrSum =>
        pcoll
          .apply(ParDo.of(MapFn[KV[K, java.lang.Iterable[V]], KV[K, java.lang.Iterable[U]]] { elem =>
            KV.of(
              elem.getKey,
              notComposedOrSum(elem.getKey, elem.getValue.asScala.toIterator).toIterable.asJava
            )
          }))
          .setCoder(KvCoder.of(OrderedSerializationCoder(ordK, kryoCoder), IterableCoder.of(kryoCoder)))
    }

  final case class Source[A](conf: Config, original: TypedSource[A], input: Option[BeamSource[A]])
      extends BeamOp[A] {
    override def runNoCache(pipeline: Pipeline): PCollection[_ <: A] =
      input match {
        case None =>
          throw new IllegalArgumentException(
            s"source $original was not connected to a beam source"
          )
        case Some(src) => src.read(pipeline, conf)
      }
  }

  final case class FromIterable[A](iterable: Iterable[A], kryoCoder: KryoCoder) extends BeamOp[A] {
    override def runNoCache(pipeline: Pipeline): PCollection[_ <: A] =
      pipeline.apply(Create.of(iterable.asJava).withCoder(kryoCoder))
  }

  final case class TransformBeamOp[A, B](
      source: BeamOp[A],
      f: PTransform[PCollection[A], PCollection[B]],
      kryoCoder: KryoCoder,
      name: String
  ) extends BeamOp[B] {
    override def runNoCache(pipeline: Pipeline): PCollection[B] = {
      val pCollection: PCollection[A] = widenPCollection(source.run(pipeline))
      pCollection.apply(name, f).setCoder(kryoCoder)
    }
  }

  final case class HashJoinTransform[K, V, U, W](
      keyCoder: Coder[K],
      joiner: (K, V, Iterable[U]) => Iterator[W]
  )(implicit kryoCoder: KryoCoder)
      extends PTransform[PCollectionTuple, PCollection[_ <: (K, W)]]("HashJoin") {

    override def expand(input: PCollectionTuple): PCollection[_ <: (K, W)] = {
      val leftPCollection = input.get("left").asInstanceOf[PCollection[(K, V)]]
      val rightPCollection = input.get("right").asInstanceOf[PCollection[(K, U)]]

      val rightPCollectionView = rightPCollection
        .apply(TupleToKV[K, U](keyCoder, kryoCoder))
        .apply(GroupByKey.create[K, U]())
        .setCoder(KvCoder.of(keyCoder, kryoCoder))
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

  final case class HashJoinOp[K, V, U, W](
      left: BeamOp[(K, V)],
      right: BeamOp[(K, U)],
      joiner: (K, V, Iterable[U]) => Iterator[W]
  )(implicit kryoCoder: KryoCoder, ordK: Ordering[K])
      extends BeamOp[(K, W)] {
    override def runNoCache(pipeline: Pipeline): PCollection[_ <: (K, W)] = {
      val leftPCollection = left.run(pipeline)
      val keyCoder: Coder[K] = OrderedSerializationCoder.apply(ordK, kryoCoder)
      val rightPCollection: PCollection[(K, U)] = widenPCollection(right.run(pipeline))

      val tuple: PCollectionTuple = PCollectionTuple.of[(K, _)](
        "left",
        widenPCollection(leftPCollection): PCollection[(K, _)],
        "right",
        widenPCollection(rightPCollection): PCollection[(K, _)]
      )

      tuple.apply(HashJoinTransform(keyCoder, joiner))
    }
  }

  final case class MergedBeamOp[A](first: BeamOp[A], second: BeamOp[A], tail: Seq[BeamOp[A]])
      extends BeamOp[A] {
    override def runNoCache(pipeline: Pipeline): PCollection[_ <: A] = {
      val collections = PCollectionList
        .of(widenPCollection(first.run(pipeline)): PCollection[A])
        .and(widenPCollection(second.run(pipeline)): PCollection[A])
        .and(tail.map(op => widenPCollection(op.run(pipeline)): PCollection[A]).asJava)

      collections.apply(Flatten.pCollections[A]())
    }
  }

  final case class CoGroupedTransform[K, V](
      joinFunction: MultiJoinFunction[K, V],
      tupleTags: Seq[TupleTag[Any]],
      keyCoder: Coder[K]
  )(implicit kryoCoder: KryoCoder)
      extends PTransform[PCollectionList[(K, Any)], PCollection[_ <: (K, V)]]("CoGrouped") {

    override def expand(collections: PCollectionList[(K, Any)]): PCollection[_ <: (K, V)] = {
      val pcols = collections.getAll.asScala.map(_.apply(TupleToKV[K, Any](keyCoder, kryoCoder)))

      val keyedPCollectionTuple: KeyedPCollectionTuple[K] = pcols
        .zip(tupleTags)
        .foldLeft(
          KeyedPCollectionTuple.empty[K](collections.getPipeline)
        )((keyed, colWithTag) => keyed.and[Any](colWithTag._2, colWithTag._1))

      keyedPCollectionTuple
        .apply(CoGroupByKey.create())
        .apply(ParDo.of(new CoGroupDoFn[K, V](joinFunction, tupleTags)))
        .setCoder(KvCoder.of(keyCoder, kryoCoder))
        .apply(KVToTuple[K, V](keyCoder, kryoCoder))
    }
  }

  final case class CoGroupedOp[K, V](
      cg: CoGrouped[K, V],
      inputOps: Seq[BeamOp[(K, Any)]]
  )(implicit kryoCoder: KryoCoder)
      extends BeamOp[(K, V)] {
    override def runNoCache(pipeline: Pipeline): PCollection[_ <: (K, V)] = {
      val keyCoder: Coder[K] = OrderedSerializationCoder.apply(cg.keyOrdering, kryoCoder)

      val pcols = inputOps.map { inputOp =>
        widenPCollection(inputOp.op.run(pipeline)): PCollection[(K, Any)]
      }

      val tupleTags = (1 to inputOps.size).map(idx => new TupleTag[Any](idx.toString))
      val joinFunction = BeamJoiner.beamMultiJoin(cg.joinFunction)

      PCollectionList
        .of(pcols.asJava)
        .apply(CoGroupedTransform(joinFunction, tupleTags, keyCoder))
    }
  }

  final case class CoGroupDoFn[K, V](
      joinFunction: MultiJoinFunction[K, V],
      tags: Seq[TupleTag[Any]]
  ) extends DoFn[KV[K, CoGbkResult], KV[K, V]] {
    @ProcessElement
    def processElement(c: DoFn[KV[K, CoGbkResult], KV[K, V]]#ProcessContext): Unit = {
      val key = c.element().getKey
      val value = c.element().getValue

      val outputIter = joinFunction(key, tags.map(t => value.getAll(t).asScala)).iterator

      while (outputIter.hasNext) {
        c.output(KV.of(key, outputIter.next()))
      }
    }
  }

  implicit final class KVOp[K, V](val op: BeamOp[(K, V)]) extends AnyVal {
    def mapGroup[U](
        reduceFn: (K, Iterator[V]) => Iterator[U]
    )(implicit ordK: Ordering[K], kryoCoder: KryoCoder): BeamOp[(K, U)] =
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
              .apply(ParDo.of(FlatMapFn[KV[K, java.lang.Iterable[U]], KV[K, U]] { elem =>
                elem.getValue.asScala.map(KV.of(elem.getKey, _))
              }))
              .setCoder(KvCoder.of(keyCoder, kryoCoder))
              .apply(KVToTuple[K, U](keyCoder, kryoCoder))
          }
        },
        kryoCoder,
        "mapGroup"
      )

    def sortedMapGroup[U](
        reduceFn: (K, Iterator[V]) => Iterator[U]
    )(implicit ordK: Ordering[K], ordV: Ordering[V], kryoCoder: KryoCoder): BeamOp[(K, U)] =
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
              .apply(ParDo.of(FlatMapFn[KV[K, java.lang.Iterable[U]], KV[K, U]] { elem =>
                elem.getValue.asScala.map(KV.of(elem.getKey, _))
              }))
              .setCoder(KvCoder.of(keyCoder, kryoCoder))
              .apply(KVToTuple[K, U](keyCoder, kryoCoder))
          }
        },
        kryoCoder,
        "sortedMapGroup"
      )

    def sorted(implicit
        ordK: Ordering[K],
        ordV: Ordering[V],
        kryoCoder: KryoCoder
    ): BeamOp[(K, V)] =
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
        kryoCoder,
        "sorted"
      )

    def mapSideAggregator(
        size: Int,
        semigroup: Semigroup[V]
    )(implicit kryoCoder: KryoCoder): BeamOp[(K, V)] =
      TransformBeamOp[(K, V), (K, V)](
        op,
        new PTransform[PCollection[(K, V)], PCollection[(K, V)]]() {
          override def expand(input: PCollection[(K, V)]): PCollection[(K, V)] =
            input.apply(ParDo.of(MapSideAggregator[K, V](size, semigroup))).setCoder(kryoCoder)
        },
        kryoCoder,
        "mapSideAggregator"
      )

    def hashJoin[U, W](
        right: BeamOp[(K, U)],
        fn: (K, V, Iterable[U]) => Iterator[W]
    )(implicit kryoCoder: KryoCoder, ord: Ordering[K]): BeamOp[(K, W)] =
      HashJoinOp(op, right, fn)
  }

  /**
   * @todo
   *   this needs to be changed to some external sorter, current Beam external sorter implementation does not
   *   provide an option to sort with custom Ordering
   * @see
   *   [[org.apache.beam.sdk.extensions.sorter.ExternalSorter]]
   */
  final case class SortGroupedValues[K, V](implicit
      ordK: Ordering[K],
      ordV: Ordering[V],
      kryoCoder: KryoCoder
  ) extends PTransform[PCollection[KV[K, java.lang.Iterable[V]]], PCollection[KV[K, java.lang.Iterable[V]]]](
        "SortGroupedValues"
      ) {
    override def expand(
        input: PCollection[KV[K, java.lang.Iterable[V]]]
    ): PCollection[KV[K, java.lang.Iterable[V]]] =
      input
        .apply(ParDo.of(MapFn[KV[K, java.lang.Iterable[V]], KV[K, java.lang.Iterable[V]]] { elem =>
          KV.of(elem.getKey, elem.getValue.asScala.toArray.sorted.toIterable.asJava)
        }))
        .setCoder(
          KvCoder.of(
            OrderedSerializationCoder(ordK, kryoCoder),
            IterableCoder.of(OrderedSerializationCoder(ordV, kryoCoder))
          )
        )
  }

  final case class TupleToKV[K, V](
      kCoder: Coder[K],
      vCoder: Coder[V]
  ) extends PTransform[PCollection[(K, V)], PCollection[KV[K, V]]]("TupleToKV") {
    override def expand(input: PCollection[(K, V)]): PCollection[KV[K, V]] =
      input
        .apply(MapElements.via[(K, V), KV[K, V]](new SimpleFunction[(K, V), KV[K, V]]() {
          override def apply(input: (K, V)): KV[K, V] = KV.of(input._1, input._2)
        }))
        .setCoder(KvCoder.of(kCoder, vCoder))
  }

  final case class KVToTuple[K, V](
      coderK: Coder[K],
      coderV: Coder[V]
  ) extends PTransform[PCollection[KV[K, V]], PCollection[(K, V)]]("KVToTuple") {
    override def expand(input: PCollection[KV[K, V]]): PCollection[(K, V)] =
      input
        .apply(MapElements.via[KV[K, V], (K, V)](new SimpleFunction[KV[K, V], (K, V)]() {
          override def apply(input: KV[K, V]): (K, V) = (input.getKey, input.getValue)
        }))
        .setCoder(TupleCoder(coderK, coderV))
  }

}
