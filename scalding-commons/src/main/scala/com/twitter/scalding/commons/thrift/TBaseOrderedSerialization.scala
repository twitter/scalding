package com.twitter.scalding.commons.thrift

import com.twitter.scalding.serialization.{ OrderedSerialization, Serialization }
import cascading.tuple.hadoop.io.BufferedInputStream
import cascading.tuple.StreamComparator
import org.apache.thrift.protocol._
import org.apache.thrift.TBase
import java.io.{ ByteArrayOutputStream, OutputStream, InputStream }
import org.apache.thrift.transport.TIOStreamTransport
import scala.reflect.ClassTag

import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

import com.twitter.scalding.serialization.JavaStreamEnrichments._

object TBaseOrderedSerialization {
  implicit def defaultTBaseOrderedSerialization[T <: TBase[_, _]: ClassTag] =
    new TBaseOrderedSerialization[T](implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]], 512)
}

class TBaseOrderedSerialization[T <: TBase[_, _]](classz: Class[T], bufsize: Int) extends TProtocolOrderedSerialization[T] {
  protected lazy val prototype: T = classz.newInstance.asInstanceOf[T]

  // Default buffer size. Ideally something like the 99%-ile size of your objects
  protected def bufferSize: Int = bufsize

  private[this] def getMinField: Short = {
    classz.getDeclaredFields.toList.filter { f =>
      java.lang.reflect.Modifier.isStatic(f.getModifiers)
    }.filter { f =>
      f.getType == classOf[org.apache.thrift.protocol.TField]
    }.map { f =>
      f.setAccessible(true)
      f.get(null).asInstanceOf[TField]
    }.map(_.id).min.toShort
  }

  // TODO: this isn't quite right -- we need min field id for all structs inside this struct
  // so we need to walk the types and check all substructs.

  override val minFieldId: Short = getMinField

  /*
    TODO: It would be great if the binary comparasion matched in the in memory for both TBase.
    In TBase the limitation is that the TProtocol can't tell a Union vs a Struct apart, and those compare differently deserialized
    */
  override def compare(a: T, b: T): Int = {
    /*
     * T <: TBase[_, _] but the first type is actually a self type, but since it is a raw
     * type in java, it breaks in scala. This cast is safe:
     */
    a.asInstanceOf[Comparable[T]].compareTo(b)
  }

  def read(from: InputStream): Try[T] = try {
    val obj = prototype.deepCopy
    // We need to have the size so we can skip on the compare
    val ignoredSize = from.readSize
    obj.read(factory.getProtocol(new TIOStreamTransport(from)))
    Success(obj.asInstanceOf[T])
  } catch {
    case NonFatal(e) => Failure(e)
  }

  def write(bb: OutputStream, t: T): Try[Unit] = try {
    /*
     * we need to be able to skip the thrift we get to a point of finishing the compare
     */
    val baos = new ByteArrayOutputStream(bufferSize)
    t.write(factory.getProtocol(new TIOStreamTransport(baos)))
    bb.writeSize(baos.size)
    baos.writeTo(bb)
    Serialization.successUnit
  } catch {
    case NonFatal(e) => Failure(e)
  }
}
