package com.twitter.scalding.commons.thrift

import com.twitter.scalding.typed.OrderedBufferable
import cascading.tuple.hadoop.io.BufferedInputStream
import cascading.tuple.StreamComparator
import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.protocol.TBinaryProtocol
import com.twitter.scrooge.ThriftStruct
import java.nio.ByteBuffer
import com.twitter.bijection.Inversion.attempt
import com.twitter.bijection.Bufferable
import org.apache.thrift.transport.TIOStreamTransport
import com.twitter.scrooge.ThriftStructCodec

abstract class ScroogeOrderedBufferable[T <: ThriftStruct] extends TProtocolOrderedBufferable[T] {

  protected def thriftStructSerializer: ThriftStructCodec[T]

  /*
    Ideally we would just disable this in memory comparasion, but Task.java in MapReduce deserializes things and uses this to determine if something
    is still the same key. For TBase we could do the full compare here, but not for ThriftStruct(Scrooge) generated code. Since its not comparable.

    TODO: It would be great if the binary comparasion matched in the in memory for both TBase and ThriftStruct.
    In ThriftStruct/Scrooge its just not comparable.
    */
  def compare(a: T, b: T) = if (a == b) 0 else -1

  def get(from: java.nio.ByteBuffer): scala.util.Try[(java.nio.ByteBuffer, T)] = attempt(from) { bb =>
    val stream = new com.esotericsoftware.kryo.io.ByteBufferInputStream(bb)
    val len = bb.getInt
    (bb, thriftStructSerializer.decode(factory.getProtocol(new TIOStreamTransport(stream))))
  }

  def put(bb: java.nio.ByteBuffer, t: T): java.nio.ByteBuffer = {
    val initialPos = bb.position
    val baos = new com.esotericsoftware.kryo.io.ByteBufferOutputStream(bb)
    bb.putInt(0)
    thriftStructSerializer.encode(t, factory.getProtocol(new TIOStreamTransport(baos)))

    val endPosition = bb.position
    val len = endPosition - (initialPos + 4)
    bb.position(initialPos)
    bb.putInt(len)
    bb.position(endPosition)
    bb
  }
}