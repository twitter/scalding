package com.twitter.scalding.beam_backend

import com.twitter.scalding.Execution.Writer
import com.twitter.scalding.typed.{Resolver, TypedSink, TypedSource}
import com.twitter.scalding.{Config, Mode, TextLine}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.values.PCollection

case class BeamMode(
    pipelineOptions: PipelineOptions,
    sources: Resolver[TypedSource, BeamSource],
    sink: Resolver[TypedSink, BeamSink]
) extends Mode {
  def newWriter(): Writer = new BeamWriter(this)
}

object BeamMode {
  def empty(pipelineOptions: PipelineOptions): BeamMode =
    BeamMode(pipelineOptions, Resolver.empty, Resolver.empty)
  def default(pipelineOptions: PipelineOptions): BeamMode =
    BeamMode(pipelineOptions, BeamSource.Default, BeamSink.Default)
}

trait BeamSource[+A] extends Serializable {
  def read(pipeline: Pipeline, config: Config): PCollection[_ <: A]
}

object BeamSource extends Serializable {
  val Default: Resolver[TypedSource, BeamSource] = {
    new Resolver[TypedSource, BeamSource] {
      def apply[A](source: TypedSource[A]): Option[BeamSource[A]] =
        source match {
          case tl: TextLine =>
            tl.localPaths match {
              case path :: Nil => Some(textLine(path))
              case _           => throw new Exception("Can not accept multiple paths to BeamSource")
            }
          case _ => None
        }
    }
  }

  def textLine(path: String): BeamSource[String] =
    new BeamSource[String] {
      override def read(pipeline: Pipeline, config: Config): PCollection[_ <: String] =
        pipeline.apply(TextIO.read().from(path))
    }
}

trait BeamSink[-A] extends Serializable {
  def write(pipeline: Pipeline, config: Config, pc: PCollection[_ <: A]): Unit
}

object BeamSink extends Serializable {
  val Default: Resolver[TypedSink, BeamSink] = {
    new Resolver[TypedSink, BeamSink] {
      def apply[A](sink: TypedSink[A]): Option[BeamSink[A]] =
        sink match {
          case tl: TextLine =>
            tl.localPaths match {
              case path :: Nil => Some(textLine(path).asInstanceOf[BeamSink[A]])
              case _           => throw new Exception("Can not accept multiple paths to BeamSink")
            }
          case _ => None
        }
    }
  }

  def textLine(path: String): BeamSink[String] =
    new BeamSink[String] {
      override def write(pipeline: Pipeline, config: Config, pc: PCollection[_ <: String]): Unit =
        pc.asInstanceOf[PCollection[String]].apply(TextIO.write().to(path))
    }
}
