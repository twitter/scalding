package com.twitter.scalding.commons.thrift

import com.twitter.scalding.serialization.{ OrderedSerialization, Serialization }
import cascading.tuple.hadoop.io.BufferedInputStream
import cascading.tuple.StreamComparator
import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.protocol.TBinaryProtocol
import com.twitter.scrooge.ThriftStruct
import java.io.{ ByteArrayOutputStream, InputStream, OutputStream }
import org.apache.thrift.transport.TIOStreamTransport
import com.twitter.scrooge.ThriftStructCodec

import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

import com.twitter.scalding.serialization.JavaStreamEnrichments._

abstract class ScroogeTProtocolOrderedSerialization[T <: ThriftStruct] extends TProtocolOrderedSerialization[T] {

  protected def thriftStructSerializer: ThriftStructCodec[T]

  // Default buffer size. Ideally something like the 99%-ile size of your objects
  protected def bufferSize: Int = 512
  /*
    Ideally we would just disable this in memory comparasion, but Task.java in MapReduce deserializes things and uses this to determine if something
    is still the same key. For TBase we could do the full compare here, but not for ThriftStruct(Scrooge) generated code. Since its not comparable.

    TODO: It would be great if the binary comparasion matched in the in memory for both TBase and ThriftStruct.
    In ThriftStruct/Scrooge its just not comparable.
    */
  def compare(a: T, b: T) = if (a == b) 0 else -1

  def read(from: InputStream): Try[T] = try {
    // We need to skip this size, even though it is here
    val ignoredSize = from.readSize
    Success(thriftStructSerializer.decode(factory.getProtocol(new TIOStreamTransport(from))))
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
