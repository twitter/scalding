package com.twitter.scalding

import com.twitter.bijection._
import com.twitter.bijection.{ Injection, AbstractInjection }
import com.twitter.bijection.Inversion._
import com.twitter.scalding._
import com.twitter.elephantbird.cascading2.scheme.LzoTextLine

import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization._
import org.json4s.{ NoTypeHints, native }

import scala.collection.JavaConverters._
import scala.util.Try

import cascading.pipe.Pipe

/**
 * This Typed JSON code is not for production usage!
 * This is only for adhoc work, mostly to be used in the repl.
 */

object TypedJson {
  implicit val formats = native.Serialization.formats(NoTypeHints)
  def caseClass2Json[A <: AnyRef](implicit tt: Manifest[A], fmt: Formats): Injection[A, String] = new AbstractInjection[A, String] {
    override def apply(a: A): String = write(a)

    override def invert(b: String): Try[A] = attempt(b)(read[A])
  }
}

case class TypedJson[T <: AnyRef: Manifest](p: String) extends FixedPathSource(p)
  with TextSourceScheme
  with SingleMappable[T]
  with TypedSink[T] {
  import Dsl._
  import TypedJson._

  val fieldSym = 'jsonString

  @transient lazy val inj = caseClass2Json[T]

  override def transformForWrite(pipe: Pipe) =
    pipe.mapTo((0) -> (fieldSym)) { inj.apply(_: T) }

  override def transformForRead(pipe: Pipe) =
    pipe.mapTo(('line) -> (fieldSym)) { (jsonStr: String) => inj.invert(jsonStr).get }

  override def hdfsScheme = HadoopSchemeInstance(new LzoTextLine().asInstanceOf[cascading.scheme.Scheme[_, _, _, _, _]])

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
