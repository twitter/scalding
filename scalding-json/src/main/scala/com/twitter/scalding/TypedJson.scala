package com.twitter.scalding

import com.twitter.bijection.{ Injection, AbstractInjection }
import com.twitter.bijection.Inversion._
import com.twitter.elephantbird.cascading3.scheme.LzoTextLine

import org.json4s._
import org.json4s.native.Serialization._
import org.json4s.{ NoTypeHints, native }

import scala.collection.JavaConverters._
import scala.util.Try

import cascading.pipe.Pipe

/**
 * This type uses the structural type of a case class, but not it's name, to describe the Json using json4s.
 * This is intended to be used for intermediate output from a REPL session.
 * The intended use is to save adhoc data between sessions.
 * The fully qualified class name of classes defined in a REPL is not stable between REPL sessions.
 *
 * We believe using a fixed schema, such as thrift or Avro is a much safer way to do long term productionized data
 * pipelines to minimize risks of incompatible changes to schema that render old data unreadable.
 */

object TypedJson {
  private implicit val formats = native.Serialization.formats(NoTypeHints)
  private def caseClass2Json[A <: AnyRef](implicit tt: Manifest[A], fmt: Formats): Injection[A, String] = new AbstractInjection[A, String] {
    override def apply(a: A): String = write(a)

    override def invert(b: String): Try[A] = attempt(b)(read[A])
  }

  def apply[T <: AnyRef: Manifest](p: String) = new TypedJson(p)
}

class TypedJson[T <: AnyRef: Manifest](p: String) extends FixedPathSource(p)
  with TextSourceScheme
  with SingleMappable[T]
  with TypedSink[T] {
  import Dsl._
  import TypedJson._

  private[this] val fieldSym = 'jsonString

  @transient private[this] lazy val inj = caseClass2Json[T]

  override def transformForWrite(pipe: Pipe) =
    pipe.mapTo((0) -> (fieldSym)) { inj.apply(_: T) }

  override def transformForRead(pipe: Pipe) =
    pipe.mapTo(('line) -> (fieldSym)) { (jsonStr: String) => inj.invert(jsonStr).get }

  override def setter[U <: T] = TupleSetter.asSubSetter[T, U](TupleSetter.singleSetter[T])

  override def toIterator(implicit config: Config, mode: Mode): Iterator[T] = {
    val tap = createTap(Read)(mode)
    mode.openForRead(config, tap)
      .asScala
      .map { te =>
        inj.invert(te.selectTuple('line).getObject(0).asInstanceOf[String]).get
      }
  }
}

case class TypedJsonLzo[T <: AnyRef: Manifest](p: String) extends TypedJson[T](p) {
  override def hdfsScheme = HadoopSchemeInstance(new LzoTextLine().asInstanceOf[cascading.scheme.Scheme[_, _, _, _, _]])
}
