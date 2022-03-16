package com.twitter.scalding.beam_backend

import com.twitter.scalding.Execution.Writer
import com.twitter.scalding.typed.{Resolver, TypedSink, TypedSource}
import com.twitter.scalding.{Config, Mode, TextLine}
import java.io.{EOFException, InputStream}
import java.nio.channels.{Channels, WritableByteChannel}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.fs.MatchResult
import org.apache.beam.sdk.io.{FileIO, FileSystems, TextIO}
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{Create, DoFn, ParDo}
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
          case TempSource(path, coder) => Some(new BeamTempFileSource(coder, path))
          case _                       => None
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
  def write(pc: PCollection[_ <: A], config: Config): Unit
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
      override def write(pc: PCollection[_ <: String], config: Config): Unit = {
        val stringPCollection: PCollection[String] = BeamFunctions.widenPCollection(pc)
        stringPCollection.apply(TextIO.write().to(path))
      }
    }
}

class BeamTempFileSink[T](output: String) extends BeamSink[T] {
  override def write(
      pc: PCollection[_ <: T],
      config: Config
  ): Unit = {
    val pColT: PCollection[T] = BeamFunctions.widenPCollection(pc)

    pColT.apply(
      FileIO
        .write()
        .via(new CoderFileSink(pColT.getCoder))
        .to(output)
    )
  }
}

class BeamTempFileSource[T](coder: Coder[T], output: String) extends BeamSource[T] {
  override def read(
      pipeline: Pipeline,
      config: Config
  ): PCollection[_ <: T] =
    pipeline
      .apply(Create.of(s"$output/*"))
      .apply(FileIO.matchAll())
      .apply(ParDo.of(new TempSourceDoFn[T](coder)))
      .setCoder(coder)
}

case class TempSourceDoFn[T](coder: Coder[T]) extends DoFn[MatchResult.Metadata, T] {
  @ProcessElement
  def processElement(c: DoFn[MatchResult.Metadata, T]#ProcessContext): Unit = {
    // We do not split the files produced in the previous stage and use a single thread per file
    val stream = Channels.newInputStream(FileSystems.open(c.element().resourceId()))
    val it = InputStreamIterator.closingIterator(stream, coder)
    while (it.hasNext) c.output(it.next())
  }
}

class CoderFileSink[T](coder: Coder[T]) extends FileIO.Sink[T] {
  private var outputStream: java.io.OutputStream = _

  override def open(channel: WritableByteChannel): Unit =
    outputStream = Channels.newOutputStream(channel)

  override def write(element: T): Unit = coder.encode(element, outputStream)
  override def flush(): Unit = outputStream.flush()
}

class InputStreamIterator[T](stream: InputStream, coder: Coder[T]) extends Iterator[T] {
  var hasNextRecord: Boolean = _
  var nextRecord: T = _

  fetchNext()
  override def hasNext: Boolean = hasNextRecord

  override def next(): T = {
    val recordToReturn = nextRecord
    fetchNext()
    recordToReturn
  }

  private def fetchNext(): Unit =
    try {
      nextRecord = coder.decode(stream)
      hasNextRecord = true
    } catch {
      case _: EOFException =>
        hasNextRecord = false
    }
}

object InputStreamIterator {
  // an empty Iterator that closes an InputStream when it is iterated
  def closingIterator[T](stream: InputStream, coder: Coder[T]): Iterator[T] = {
    def closeIt(is: InputStream): Iterator[T] =
      new Iterator[T] {
        def hasNext: Boolean = {
          is.close()
          false
        }
        def next = Iterator.empty.next
      }
    new InputStreamIterator[T](stream, coder) ++ closeIt(stream)
  }
}
