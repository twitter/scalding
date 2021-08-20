package com.twitter.scalding.beam_backend

import com.twitter.scalding.Config
import com.twitter.scalding.beam_backend.BeamFunctions._
import com.twitter.scalding.typed.TypedSource
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.values.PCollection
import scala.collection.JavaConverters._

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

  final case class TransformBeamOp[A, B](source: BeamOp[A], f: PTransform[PCollection[A], PCollection[B]], kryoCoder: KryoCoder)() extends BeamOp[B] {
    def run(pipeline: Pipeline): PCollection[B] = {
      source.run(pipeline).asInstanceOf[PCollection[A]].apply(f).setCoder(kryoCoder)
    }
  }
}
