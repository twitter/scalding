package com.twitter.scalding.commons.thrift

import com.twitter.scalding.typed.OrderedBinary
import cascading.tuple.hadoop.io.BufferedInputStream
import cascading.tuple.StreamComparator
import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.TBase
import java.io.{ ByteArrayOutputStream, OutputStream, InputStream }
import com.twitter.bijection.Inversion.attempt
import com.twitter.bijection.Bufferable
import org.apache.thrift.transport.TIOStreamTransport

import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

import com.twitter.scalding.serialization.JavaStreamEnrichments._

abstract class TBaseOrderedBufferable[T <: TBase[_, _]] extends TProtocolOrderedBinary[T] {

  @transient protected def prototype: T
  // Default buffer size. Ideally something like the 99%-ile size of your objects
  protected def bufferSize: Int

  /*
    TODO: It would be great if the binary comparasion matched in the in memory for both TBase and ThriftStruct.
    In TBase the limitation is that the TProtocol can't tell a Union vs a Struct apart, and those compare differently deserialized
    */
  def compare(a: T, b: T): Int

  def get(from: InputStream): Try[T] = try {
    val obj = prototype.deepCopy
    // We need to have the size so we can skip on the compare
    val ignoredSize = from.readSize
    obj.read(factory.getProtocol(new TIOStreamTransport(from)))
    Success(obj.asInstanceOf[T])
  } catch {
    case NonFatal(e) => Failure(e)
  }

  def put(bb: OutputStream, t: T): Try[Unit] = try {
    /*
     * we need to be able to skip the thrift we get to a point of finishing the compare
     */
    val baos = new ByteArrayOutputStream(bufferSize)
    t.write(factory.getProtocol(new TIOStreamTransport(baos)))
    bb.writeSize(baos.size)
    baos.writeTo(bb)
    OrderedBinary.successUnit
  } catch {
    case NonFatal(e) => Failure(e)
  }
}
